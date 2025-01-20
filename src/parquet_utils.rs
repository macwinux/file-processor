pub mod utils {
    use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    use arrow::datatypes::Schema;
    
    
    pub fn read_parquet(path: &str) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        let file = std::fs::File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        println!("Converted arrow schema is: {}", builder.schema());
        let reader = builder.build().unwrap();
        Ok(reader.collect::<Result<Vec<_>,_>>()?)
    
    }
    
    pub fn write_parquet (
        path: &str,
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file = std::fs::File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        for row in batches{
            writer.write(&row)?;
        }
        writer.close()?;
        Ok(())
    }
    
    #[allow(dead_code)]
    pub fn write_example_parquet(path: &str) {
        let col_ages = Arc::new(Int64Array::from_iter_values([11, 22, 33])) as ArrayRef;
        let col_names = Arc::new(StringArray::from_iter_values(["Pepe", "Juan", "Luis"])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("age", col_ages), ("names", col_names)]).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), None).unwrap();
        writer.write(&to_write).unwrap();
        writer.close().unwrap();
    }
}

#[cfg(test)]
mod test {
    use arrow::array::{Int64Array, StringArray};

    use super::utils::{read_parquet, write_example_parquet};

    #[test]
    fn write_parquet_and_read() {
        write_example_parquet("example.parquet");
        let records = read_parquet("example.parquet").unwrap();
        let result_names = vec!["Pepe", "Juan", "Luis"];
        let result_ages = vec![11, 22, 33];
        for record in records {
            for row in 0..record.num_rows() {
                let column_age =  record.column_by_name("age").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
                let column_name =  record.column_by_name("names").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
                let value_age = column_age.value(row);
                let value_name = column_name.value(row);
                assert_eq!(value_age,result_ages[row]);
                assert_eq!(value_name,result_names[row]);
            }
        }
    }
}
