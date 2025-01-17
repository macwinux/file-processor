
use arrow::{array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray}, ipc::Int};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde::Deserialize;
use std::fs;
use std::sync::Arc;
use serde_yaml;
use arrow::datatypes::{Float64Type, Schema};

#[derive(Debug, Deserialize)]
struct Transformation {
    column: String,
    action: String,
    value: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct Config {
    input: String,
    output: String,
    transformations: Vec<Transformation>,
}

fn read_config(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(path)?;
    let config: Config = serde_yaml::from_str(&content)?;
    Ok(config)
}

fn read_parquet(path: &str) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let file = fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    println!("Converted arrow schema is: {}", builder.schema());
    let reader = builder.build().unwrap();
    Ok(reader.collect::<Result<Vec<_>,_>>()?)

}

fn write_parquet (
    path: &str,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    for row in batches{
        writer.write(&row)?;
    }
    writer.close()?;
    Ok(())
}

fn write_example_parquet() {
    let col_ages = Arc::new(Int64Array::from_iter_values([11, 22, 33])) as ArrayRef;
    let col_names = Arc::new(StringArray::from_iter_values(["Pepe", "Juan", "Luis"])) as ArrayRef;
    let to_write = RecordBatch::try_from_iter([("age", col_ages), ("names", col_names)]).unwrap();
    let file = fs::File::create("example.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file, to_write.schema(), None).unwrap();
    writer.write(&to_write).unwrap();
    writer.close().unwrap();
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

   let records = read_parquet("example.parquet").unwrap();
   
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
   write_parquet("output.parquet", schema, records_write).unwrap();
}
