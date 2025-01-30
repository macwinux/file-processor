use arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::{dataframe::DataFrameWriteOptions, prelude::SessionContext};
use std::{fs, rc::Rc};
mod parquet;
mod models;
mod transformations;
use models::configurations::{Config, Result};
use transformations::transformer::Transformer;

fn read_config(path: &str) -> Result<Config> {
    let content = fs::read_to_string(path)?;
    let config: Config = serde_yaml::from_str(&content)?;
    Ok(config)
}

#[tokio::main]
async fn main() {


    let yaml: Config = read_config("config.yaml").unwrap();

    println!("input: {}", yaml.input.path);
    println!("output: {}", yaml.output.path);
    let output_path = yaml.output.path.clone();
    let ctx = Rc::new(SessionContext::new());

    let transformer: Transformer = Transformer::new(yaml.input, yaml.output, Rc::clone(&ctx)).await;
    
    let check = transformer.check().await;
    match check {
        Ok(()) => {println!("El formato es correcto")},
        Err(err) => {panic!("{}", err)}
    };
    //ctx.register_parquet("input_table", input_path,ParquetReadOptions::default()).await.unwrap();
    println!("Ejectuando consulta: {}", yaml.sql);
    let query = format!("{}{}",yaml.sql,yaml.table);
    let df = ctx.sql(&query).await.unwrap();

    df.write_parquet(&output_path, DataFrameWriteOptions::default(),None).await.unwrap();

   let records:Vec<RecordBatch> = parquet::utils::read_parquet(&output_path).unwrap();

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