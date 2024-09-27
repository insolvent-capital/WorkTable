use std::collections::HashMap;
use proc_macro2::{Ident, TokenTree};
use syn::spanned::Spanned;

use crate::worktable::model::Operation;
use crate::worktable::parser::Parser;

impl Parser {
    pub fn parse_operations(&mut self) -> syn::Result<HashMap<Ident, Operation>> {
        let mut ops = HashMap::new();
        while self.has_next() {
            let row = self.parse_operation()?;
            if ops.get(&row.name).is_some() {
                return Err(syn::Error::new(
                    row.name.span(),
                    "Non-unique query name",
                ));
            }
            ops.insert(row.name.clone(), row);
            self.try_parse_comma()?
        }
        Ok(ops)
    }

    pub fn parse_operation(&mut self) -> syn::Result<Operation> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected operation name in declaration",
        ))?;
        let name = if let TokenTree::Ident(ident) = ident {
            ident
        } else {
            return Err(syn::Error::new(
                ident.span(),
                "Expected field name identifier.",
            ));
        };

        let columns = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected column identifiers in declaration",
        ))?;
        let columns = if let TokenTree::Group(columns) = columns {
            let mut parser = Parser::new(columns.stream());
            let mut columns = Vec::new();
            while parser.has_next() {
                let column = parser.parse_column_ident()?;
                columns.push(column);
                parser.try_parse_comma()?;
            }
            columns
        } else {
            return Err(syn::Error::new(
                columns.span(),
                "Expected column identifiers in declaration",
            ));
        };

        let by = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected operation name in declaration",
        ))?;
        if let TokenTree::Ident(by) = by {
            if by.to_string().as_str() != "by" {
                return Err(syn::Error::new(
                    by.span(),
                    "Expected `by` identifier",
                ));
            }
        } else {
            return Err(syn::Error::new(
                by.span(),
                "Expected `by` identifier.",
            ));
        };

        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected operation name in declaration",
        ))?;
        let by_name = if let TokenTree::Ident(ident) = ident {
            ident
        } else {
            return Err(syn::Error::new(
                ident.span(),
                "Expected by name identifier.",
            ));
        };

        Ok(Operation {
            name,
            columns,
            by: by_name
        })
    }

    pub fn parse_column_ident(&mut self) -> syn::Result<Ident> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected operation name in declaration",
        ))?;
        if let TokenTree::Ident(ident) = ident {
            Ok(ident)
        } else {
            Err(syn::Error::new(
                ident.span(),
                "Expected field name identifier.",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::worktable::parser::Parser;

    #[test]
    fn test_operation() {
        let tokens =  quote! {
            TestQuery(id, test) by name,
        };

        let mut parser = Parser::new(tokens);
        let op = parser.parse_operation().unwrap();
        assert_eq!(op.name.to_string(), "TestQuery".to_string());
        assert_eq!(op.columns.len(), 2);
        assert_eq!(op.columns[0], "id".to_string());
        assert_eq!(op.columns[1], "test".to_string());
        assert_eq!(op.by.to_string(), "name".to_string());
    }
}