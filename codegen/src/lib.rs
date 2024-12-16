mod name_generator;
mod persist_index;
mod persist_table;
mod worktable;

use proc_macro::TokenStream;

// TODO: Refactor this codegen stuff because it's now too strange.

#[proc_macro]
pub fn worktable(input: TokenStream) -> TokenStream {
    worktable::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro_derive(PersistIndex)]
pub fn persist_index(input: TokenStream) -> TokenStream {
    persist_index::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro_derive(PersistTable)]
pub fn persist_table(input: TokenStream) -> TokenStream {
    persist_table::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
