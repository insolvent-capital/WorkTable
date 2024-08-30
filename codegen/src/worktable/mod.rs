use proc_macro2::TokenStream;
use quote::quote;

use gen_row_type::gen_row_def;
use gen_table_type::gen_table_def;
use parse_columns::parse_columns;
use parse_name::parse_name;
use crate::worktable::gen_index_type::{gen_impl_def, gen_index_def};
use crate::worktable::gen_table_type::gen_table_index_impl;

mod gen_row_type;
mod gen_table_type;
mod parse_columns;
mod parse_name;
mod parse_punct;
mod gen_index_type;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let mut i = input.clone().into_iter();

    let name = parse_name(&mut i, &input)?;
    let columns = parse_columns(&mut i, &input)?;

    let pk_type = columns
        .columns_map
        .get(&columns.primary_key)
        .expect("exists")
        .clone();

    let (row_def, row_ident) = gen_row_def(columns.clone(), name.clone());
    let (index_def, index_ident) = gen_index_def(columns.clone(), &name, &row_ident);
    let (table_def, table_ident) = gen_table_def(&name, &pk_type, &row_ident, &index_ident);
    let table_index_impl = gen_table_index_impl(columns, &table_ident, &row_ident);

    Ok(TokenStream::from(quote! {
        #row_def

        #index_def

        #table_def

        #table_index_impl
    }))
}