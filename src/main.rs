use arrow::array::{Int64Array, RecordBatch, StringArray};
use std::fs;
mod parquet_utils;
mod models;
use models::configurations::Config;

fn read_config(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(path)?;
    let config: Config = serde_yaml::from_str(&content)?;
    Ok(config)
}

#[tokio::main]
async fn main() {


    let yaml: Config = read_config("config.yaml").unwrap();

    println!("input: {}", yaml.input);
    println!("output: {}", yaml.output);
    for tranformation in yaml.transformations {
        println!("transformation column: {}", tranformation.column);
        println!("transformation action: {}", tranformation.action);
        if tranformation.value.is_some() {
            println!("transformation value: {}", tranformation.value.unwrap());
        }
    }

   let records:Vec<RecordBatch> = parquet_utils::utils::read_parquet("example.parquet").unwrap();
   
   let schema = records[0].schema();
   let records_write = records.clone();
   for record in records {
        println!("Records {}", record.num_rows());
        for row in 0..record.num_rows() {
            
            let column_age =  record.column_by_name("age").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
            let column_name =  record.column_by_name("names").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
            let value_age = column_age.value(row);
            let value_name = column_name.value(row);
            println!("------User number {}------", row+1);
            println!("\tAge: {}",value_age);
            println!("\tName: {}", value_name);
        }
   }
   parquet_utils::utils::write_parquet("output.parquet", schema, records_write).unwrap();
}