use arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::{dataframe::DataFrameWriteOptions, prelude::{ParquetReadOptions, SessionContext}};
use std::fs;
mod parquet;
mod models;
use models::configurations::{Config, Result};


fn read_config(path: &str) -> Result<Config> {
    let content = fs::read_to_string(path)?;
    let config: Config = serde_yaml::from_str(&content)?;
    Ok(config)
}

#[tokio::main]
async fn main() {


    let yaml: Config = read_config("config.yaml").unwrap();

    println!("input: {}", yaml.input);
    println!("output: {}", yaml.output);

    let ctx = SessionContext::new();

    ctx.register_parquet("input_table", yaml.input,ParquetReadOptions::default()).await.unwrap();
    println!("Ejectuando consulta: {}", yaml.sql);
    let query = format!("{}{}",yaml.sql,yaml.table);
    let df = ctx.sql(&query).await.unwrap();

    df.write_parquet(&yaml.output, DataFrameWriteOptions::default(),None).await.unwrap();

   let records:Vec<RecordBatch> = parquet::utils::read_parquet(&yaml.output).unwrap();

   for record in &records {
       for row in 0..record.num_rows() {
           let column_age =  record.column_by_name("age").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
           let column_name =  record.column_by_name("name").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
           let value_age = column_age.value(row);
           let value_name = column_name.value(row);
           println!("------User number {}------", row+1);
           println!("\tAge: {}",value_age);
           println!("\tName: {}", value_name);
       }
    }
}