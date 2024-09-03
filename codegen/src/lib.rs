mod worktable;

use proc_macro::TokenStream;
use syn::spanned::Spanned;

#[proc_macro]
pub fn worktable(input: TokenStream) -> TokenStream {
    worktable::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
