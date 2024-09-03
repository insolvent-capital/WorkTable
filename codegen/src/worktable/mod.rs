use proc_macro2::TokenStream;
use quote::quote;

mod parser;
mod generator;
mod model;

pub use parser::Parser;
use crate::worktable::generator::Generator;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let mut parser = Parser::new( input);

    let name = parser.parse_name()?;
    let mut columns = parser.parse_columns()?;
    let indexes = parser.parse_indexes()?;
    columns.indexes = indexes;

    let mut generator = Generator::new(name, columns);

    let pk_def = generator.gen_pk_def();
    let row_def = generator.gen_row_def();
    let wrapper_def = generator.gen_wrapper_def();
    let wrapper_impl = generator.gen_wrapper_impl();
    let index_def = generator.gen_index_def();
    let table_def = generator.gen_table_def();
    let table_index_impl = generator.gen_table_index_impl()?;

    Ok(TokenStream::from(quote! {
        #pk_def
        #row_def
        #wrapper_def
        #wrapper_impl
        #index_def
        #table_def
        #table_index_impl
    }))
}

#[cfg(test)]
mod test {
    use quote::quote;
    use crate::worktable::expand;

    #[test]
    fn test() {
        let tokens = quote! {
            name: Test,
        columns: {
            id: u64 primary_key,
            test: i64,
            exchnage: String
        },
        indexes: {
            test_idx: test,
            exchnage_idx: exchange
        }
        };

        let res = expand(tokens).unwrap();
    }
}