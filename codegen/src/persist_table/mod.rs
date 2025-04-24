use proc_macro2::TokenStream;
use quote::quote;

use crate::persist_table::generator::Generator;
use crate::persist_table::parser::Parser;

mod generator;
mod parser;

pub use generator::WT_INDEX_EXTENSION;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input_fn = Parser::parse_struct(input)?;
    let pk_ident = Parser::parse_pk_ident(&input_fn);
    let attributes = Parser::parse_attributes(&input_fn.attrs);

    let generator = Generator {
        struct_def: input_fn,
        pk_ident,
        attributes,
    };

    let space_file_def = generator.gen_space_file_def();
    let persistence_engine = generator.get_persistence_engine_type();
    let persistence_task = generator.get_persistence_task_type();

    Ok(quote! {
        #space_file_def
        #persistence_engine
        #persistence_task
    })
}
