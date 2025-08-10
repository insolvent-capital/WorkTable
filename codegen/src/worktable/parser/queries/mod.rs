mod delete;
mod in_place;
mod operation;
mod select;
mod update;

use proc_macro2::TokenTree;
use syn::spanned::Spanned;

use crate::worktable::Parser;
use crate::worktable::model::Queries;

impl Parser {
    pub fn parse_queries(&mut self) -> syn::Result<Queries> {
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

        let mut queries = Queries::default();
        let ops = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected operation declarations",
        ))?;
        if let TokenTree::Group(ops) = ops {
            let mut parser = Parser::new(ops.stream());
            while let Some(ident) = parser.peek_next() {
                match ident.to_string().as_str() {
                    "update" => {
                        let updates = parser.parse_updates()?;
                        queries.updates = updates;
                    }
                    "delete" => {
                        let deletes = parser.parse_deletes()?;
                        queries.deletes = deletes;
                    }
                    "in_place" => {
                        let in_place = parser.parse_in_place()?;
                        queries.in_place = in_place;
                    }
                    _ => return Err(syn::Error::new(ident.span(), "Unexpected identifier")),
                }
            }
        } else {
            return Err(syn::Error::new(
                ops.span(),
                "Expected operation declarations",
            ));
        };

        self.try_parse_comma()?;

        Ok(queries)
    }
}
