mod gen_row_type;
mod gen_table_type;
mod parse_columns;
mod parse_name;
mod parse_punct;
mod worktable;

use proc_macro::TokenStream;

#[proc_macro]
pub fn worktable(input: TokenStream) -> TokenStream {
    worktable::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
