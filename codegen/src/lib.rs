
mod worktable;
mod worktable_row;

use syn::spanned::Spanned;
use proc_macro::TokenStream;

#[proc_macro]
pub fn worktable(input: TokenStream) -> TokenStream {
    worktable::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro_derive(WorktableRow)]
pub fn worktable_row(input: TokenStream) -> TokenStream {
    worktable_row::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
