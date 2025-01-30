pub mod transformer {


    use std::rc::Rc;

    use crate::models::configurations::{File,FormatError, Result};
    use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionContext};
    //pub trait CheckerFiles {
    //    async fn new(input: File, output: File, context: Rc<SessionContext>) -> Transformer;
    //    async fn check(&self) -> Result<()>;
    //    async fn transform(&self) -> ();
    //}

    #[allow(dead_code)]
    pub struct Transformer{
        pub input: File,
        pub output: File,
        pub context: Rc<SessionContext>,
        pub table: String,
    }

    impl Transformer {

        pub async fn new(input: File, output: File, table: String, context: Rc<SessionContext>) -> Transformer {
            Transformer{input, output, context, table}
        }

        pub async fn check(&self) -> Result<()> {
            let value = self.input.format.as_str(); 
            match value {
                "parquet" => {
                    println!("Table registered");
                    self.context.register_parquet(&self.table, &self.input.path,ParquetReadOptions::default()).await.unwrap();
                    Ok(())
                }
                _ => {
                    Err(Box::new(FormatError(format!("No format {} allowed", value))))
                }
            }
        }
        
        pub async fn transform(&self, query: String) -> DataFrame {
            let sql = format!("{}{}",&query, &self.table);
            self.context.as_ref().sql(&sql).await.unwrap()
        }
    }

}