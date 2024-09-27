use std::collections::HashMap;

use proc_macro2::{Ident, TokenTree};
use syn::spanned::Spanned;

use crate::worktable::model::Operation;
use crate::worktable::Parser;

impl Parser {
    pub fn parse_deletes(&mut self) -> syn::Result<HashMap<Ident, Operation>> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected `delete` field in declaration",
        ))?;
        if let TokenTree::Ident(ident) = ident {
            if ident.to_string().as_str() != "delete" {
                return Err(syn::Error::new(
                    ident.span(),
                    "Expected `delete` field",
                ));
            }
        } else {
            return Err(syn::Error::new(
                ident.span(),
                "Expected field name identifier.",
            ));
        };

        self.parse_colon()?;

        let ops = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected operation declarations",
        ))?;
        if let TokenTree::Group(ops) = ops {
            let mut parser = Parser::new(ops.stream());
            parser.parse_operations()
        } else {
            Err(syn::Error::new(
                ops.span(),
                "Expected operation declarations",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::{Ident, Span};
    use quote::quote;

    use crate::worktable::Parser;

    #[test]
    fn test_update() {
        let tokens = quote! {
            update: {
                TestQuery(id, test) by name,
                Test1Query(id, name) by test,
            }
        };
        let mut parser = Parser::new(tokens);
        let ops = parser.parse_updates().unwrap();

        assert_eq!(ops.len(), 2);
        let op = ops.get(&Ident::new("TestQuery", Span::mixed_site())).unwrap();

        assert_eq!(op.name, "TestQuery");
        assert_eq!(op.columns.len(), 2);
        assert_eq!(op.columns[0], "id");
        assert_eq!(op.columns[1], "test");
        assert_eq!(op.by, "name");
    }
}