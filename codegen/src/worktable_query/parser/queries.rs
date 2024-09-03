use proc_macro2::TokenTree;
use syn::spanned::Spanned;

use crate::worktable::Parser;

impl Parser {
    pub fn parse_queries(&mut self) -> syn::Result<()> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected `queries` field in declaration",
        ))?;
        if let TokenTree::Ident(ident) = ident {
            if ident.to_string().as_str() != "queries" {
                return Err(syn::Error::new(
                    ident.span(),
                    "Expected `queries` field. `WorkTable` name must be specified",
                ));
            }
        } else {
            return Err(syn::Error::new(
                ident.span(),
                "Expected field name identifier.",
            ));
        };

        self.parse_colon()?;

        todo!()
    }
}