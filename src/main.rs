use std::{env, error::Error};

use aws_sdk_s3::{
    types::{
        InputSerialization, JsonInput, JsonOutput, OutputSerialization,
        SelectObjectContentEventStream,
    },
    Client,
};
use serde::{de::IgnoredAny, Deserialize};
use serde_json::from_str;

#[derive(Deserialize, Debug)]
pub struct Record {
    pub id: i32,
    pub name: String,
}

type AppError = Box<dyn Error>;

fn parse_line_bufferd(buf: &mut String, line: &str) -> Result<Option<Record>, AppError> {
    if buf.is_empty() && is_valid_json(line) {
        Ok(Some(from_str(line)?))
    } else {
        buf.push_str(line);
        if is_valid_json(&buf) {
            let record = from_str(buf)?;
            buf.clear();
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }
}

fn is_valid_json(data: impl AsRef<str>) -> bool {
    from_str::<IgnoredAny>(data.as_ref()).is_ok()
}

async fn handler(client: &Client) -> Result<Vec<Record>, AppError> {
    let bucket_name = match env::var("BUCKET_NAME") {
        Ok(val) => val,
        Err(_) => panic!("BUCKET_NAME is not set"),
    };
    let object_key = match env::var("OBJECT_KEY") {
        Ok(val) => val,
        Err(_) => panic!("OBJECT_KEY is not set"),
    };

    let mut output = match client
        .select_object_content()
        .bucket(bucket_name)
        .key(object_key)
        .expression_type(aws_sdk_s3::types::ExpressionType::Sql)
        .expression("SELECT * FROM s3object s WHERE s.name LIKE '%b%'")
        .input_serialization(
            InputSerialization::builder()
                .json(
                    JsonInput::builder()
                        .r#type(aws_sdk_s3::types::JsonType::Lines)
                        .build(),
                )
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .json(JsonOutput::builder().build())
                .build(),
        )
        .send()
        .await
    {
        Ok(val) => val,
        Err(e) => panic!("Error: {:?}", e),
    };

    let mut processed_records: Vec<Record> = vec![];
    let mut buf = String::new();

    while let Some(event) = output.payload.recv().await? {
        if let SelectObjectContentEventStream::Records(records) = event {
            let records_str = records.payload().map(|p| p.as_ref()).unwrap_or_default();
            let records_str = std::str::from_utf8(records_str).expect("invalid utf8");
            for line in records_str.lines() {
                if let Some(record) = parse_line_bufferd(&mut buf, line)? {
                    processed_records.push(record);
                }
            }
        }
    }

    Ok(processed_records)
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    let records = handler(&client).await?;
    println!("{:?}", records);
    Ok(())
}
