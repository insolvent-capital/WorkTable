use crate::worktable::model::Index;
use crate::worktable::Parser;
use proc_macro2::{Delimiter, Ident, TokenTree};
use std::collections::HashMap;
use syn::spanned::Spanned;

impl Parser {
    pub fn parse_indexes(&mut self) -> syn::Result<HashMap<Ident, Index>> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected `indexes` field in declaration",
        ))?;

        if let TokenTree::Ident(ident) = ident {
            if ident.to_string().as_str() != "indexes" {
                return Err(syn::Error::new(ident.span(), "Expected `indexes` field"));
            }
        } else {
            return Err(syn::Error::new(
                ident.span(),
                "Expected field name identifier.",
            ));
        };

        self.parse_colon()?;

        let tt = {
            let group = self.input_iter.next().ok_or(syn::Error::new(
                self.input.span(),
                "Expected `indexes` declarations",
            ))?;
            if let TokenTree::Group(group) = group {
                if group.delimiter() != Delimiter::Brace {
                    return Err(syn::Error::new(group.span(), "Expected brace"));
                }
                group.stream()
            } else {
                return Err(syn::Error::new(
                    group.span(),
                    "Expected `indexes` declarations",
                ));
            }
        };

        let mut parser = Parser::new(tt);

        let mut rows = HashMap::new();
        let mut ind = true;

        while ind {
            let (name, row) = parser.parse_index()?;
            rows.insert(name, row);
            ind = parser.has_next()
        }

        self.try_parse_comma()?;

        Ok(rows)
    }

    pub fn parse_index(&mut self) -> syn::Result<(Ident, Index)> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected index name field in declaration",
        ))?;
        let ident = if let TokenTree::Ident(ident) = ident {
            ident
        } else {
            return Err(syn::Error::new(ident.span(), "Expected index name"));
        };

        self.parse_colon()?;

        let row_name = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected row name field in declaration",
        ))?;
        let row_name = if let TokenTree::Ident(row_name) = row_name {
            row_name
        } else {
            return Err(syn::Error::new(row_name.span(), "Expected row name"));
        };

        let is_unique = if let Some(tt) = self.input_iter.peek() {
            match tt {
                TokenTree::Ident(index) => {
                    if index.to_string().as_str() == "unique" {
                        self.input_iter.next();
                        true
                    } else {
                        return Err(syn::Error::new(index.span(), "Unexpected identifier."));
                    }
                }
                TokenTree::Punct(comma) => {
                    if comma.as_char() != ',' {
                        return Err(syn::Error::new(
                            comma.span(),
                            format!("Expected `,` found: `{}`", comma.as_char()),
                        ));
                    }
                    self.input_iter.next();
                    false
                }
                _ => false,
            }
        } else {
            false
        };

        self.try_parse_comma()?;

        Ok((
            row_name.clone(),
            Index {
                name: ident,
                field: row_name,
                is_unique,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::worktable::model::Index;
    use crate::worktable::Parser;
    use proc_macro2::{Ident, Span, TokenStream};
    use quote::quote;

    #[test]
    fn test_indexes_parse() {
        let tokens = TokenStream::from(quote! {indexes: {
            id_idx: id unique,
            test_idx: test,
        }});
        let mut parser = Parser::new(tokens);
        let columns = parser.parse_indexes();

        assert!(columns.is_ok());
        let columns = columns.unwrap();
    }
}
