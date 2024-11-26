use proc_macro2::{Delimiter, TokenTree};
use syn::spanned::Spanned as _;

use crate::worktable::model::{Columns, GeneratorType, Row};
use crate::worktable::Parser;

impl Parser {
    pub fn parse_columns(&mut self) -> syn::Result<Columns> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected `columns` field in declaration",
        ))?;
        if let TokenTree::Ident(ident) = ident {
            if ident.to_string().as_str() != "columns" {
                return Err(syn::Error::new(
                    ident.span(),
                    "Expected `columns` field. `WorkTable` name must be specified",
                ));
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
                "Expected `columns` declarations",
            ))?;
            if let TokenTree::Group(group) = group {
                if group.delimiter() != Delimiter::Brace {
                    return Err(syn::Error::new(group.span(), "Expected brace"));
                }
                group.stream()
            } else {
                return Err(syn::Error::new(
                    group.span(),
                    "Expected `columns` declarations",
                ));
            }
        };
        let mut parser = Parser::new(tt);
        let mut rows = Vec::new();
        while parser.has_next() {
            let row = parser.parse_row()?;
            rows.push(row);
        }

        self.try_parse_comma()?;

        Columns::try_from_rows(rows, &self.input)
    }

    fn parse_row(&mut self) -> syn::Result<Row> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected column name in declaration",
        ))?;
        let name = if let TokenTree::Ident(ident) = ident {
            ident
        } else {
            return Err(syn::Error::new(ident.span(), "Expected identifier."));
        };

        self.parse_colon()?;

        let type_ = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected column type in declaration",
        ))?;
        let type_ = if let TokenTree::Ident(type_) = type_ {
            type_
        } else {
            return Err(syn::Error::new(type_.span(), "Expected type."));
        };

        let is_primary_key = if let Some(TokenTree::Ident(index)) = self.input_iter.peek() {
            if index.to_string().as_str() == "primary_key" {
                self.input_iter.next();
                true
            } else {
                false
            }
        } else {
            false
        };

        let gen_type = if let Some(TokenTree::Ident(index)) = self.input_iter.peek() {
            if index.to_string().as_str() == "autoincrement" {
                self.input_iter.next();
                GeneratorType::Autoincrement
            } else if index.to_string().as_str() == "custom" {
                self.input_iter.next();
                GeneratorType::Custom
            } else {
                GeneratorType::None
            }
        } else {
            GeneratorType::None
        };

        let optional = if let Some(TokenTree::Ident(option)) = self.input_iter.peek() {
            if option.to_string().as_str() == "optional" {
                self.input_iter.next();
                true
            } else {
                return Err(syn::Error::new(option.span(), "Unexpected identifier."));
            }
        } else {
            false
        };

        self.try_parse_comma()?;

        Ok(Row {
            name,
            type_,
            is_primary_key,
            gen_type,
            optional,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use proc_macro2::TokenStream;
    use quote::quote;

    use crate::worktable::Parser;

    #[test]
    fn test_columns_parse() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64 primary_key,
            test: u64,
        }});
        let mut parser = Parser::new(tokens);
        let columns = parser.parse_columns();

        assert!(columns.is_ok());
        let columns = columns.unwrap();

        assert_eq!(columns.primary_keys[0].to_string(), "id");

        let map: HashMap<_, _> = columns
            .columns_map
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(map.get("id"), Some(&"i64".to_string()));
        assert_eq!(map.get("test"), Some(&"u64".to_string()));
    }

    #[test]
    fn test_columns_parse_no_last_comma() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64 primary_key,
            test: u64
        }});
        let mut parser = Parser::new(tokens);
        let columns = parser.parse_columns();

        assert!(columns.is_ok());
        let columns = columns.unwrap();

        assert_eq!(columns.primary_keys[0].to_string(), "id");

        let map: HashMap<_, _> = columns
            .columns_map
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(map.get("id"), Some(&"i64".to_string()));
        assert_eq!(map.get("test"), Some(&"u64".to_string()));
    }

    #[test]
    fn test_columns_parse_optional() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64 primary_key,
            test: u64 optional,
        }});
        let mut parser = Parser::new(tokens);
        let columns = parser.parse_columns();

        let columns = columns.unwrap();

        assert_eq!(columns.primary_keys[0].to_string(), "id");

        let map: HashMap<_, _> = columns
            .columns_map
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(map.get("id"), Some(&"i64".to_string()));
        assert_eq!(
            map.get("test"),
            Some(&"core :: option :: Option < u64 >".to_string())
        );
    }

    #[test]
    fn test_columns_parse_three() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64 primary_key,
            test: u64,
            a: u64
        }});
        let mut parser = Parser::new(tokens);
        let columns = parser.parse_columns();

        let columns = columns.unwrap();

        assert_eq!(columns.primary_keys[0].to_string(), "id");

        let map: HashMap<_, _> = columns
            .columns_map
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(map.get("id"), Some(&"i64".to_string()));
        assert_eq!(map.get("test"), Some(&"u64".to_string()));
        assert_eq!(map.get("a"), Some(&"u64".to_string()));
    }

    #[test]
    fn test_columns_parse_no_primary_key() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64,
            test: u64
        }});
        let mut parser = Parser::new(tokens);
        let columns = parser.parse_columns();

        assert!(columns.is_err());
    }

    mod row {
        use super::*;

        #[test]
        fn test_row_parse() {
            let row_tokens = TokenStream::from(quote! {id: i64 primary_key,});
            let iter = &mut row_tokens.clone().into_iter();

            let mut parser = Parser::new(row_tokens);
            let row = parser.parse_row();

            assert!(row.is_ok());
            let row = row.unwrap();

            assert_eq!(row.name.to_string(), "id");
            assert_eq!(row.type_.to_string(), "i64");
            assert!(row.is_primary_key)
        }

        #[test]
        fn test_row_parse_no_comma() {
            let row_tokens = TokenStream::from(quote! {id: i64 primary_key});
            let iter = &mut row_tokens.clone().into_iter();

            let mut parser = Parser::new(row_tokens);
            let row = parser.parse_row();

            assert!(row.is_ok());
            let row = row.unwrap();

            assert_eq!(row.name.to_string(), "id");
            assert_eq!(row.type_.to_string(), "i64");
            assert!(row.is_primary_key)
        }

        #[test]
        fn test_row_parse_no_primary_key() {
            let row_tokens = TokenStream::from(quote! {id: i64,});
            let iter = &mut row_tokens.clone().into_iter();

            let mut parser = Parser::new(row_tokens);
            let row = parser.parse_row();

            assert!(row.is_ok());
            let row = row.unwrap();

            assert_eq!(row.name.to_string(), "id");
            assert_eq!(row.type_.to_string(), "i64");
            assert!(!row.is_primary_key)
        }

        #[test]
        fn test_row_parse_no_primary_key_no_comma() {
            let row_tokens = TokenStream::from(quote! {id: i64});
            let iter = &mut row_tokens.clone().into_iter();

            let mut parser = Parser::new(row_tokens);
            let row = parser.parse_row();

            assert!(row.is_ok());
            let row = row.unwrap();

            assert_eq!(row.name.to_string(), "id");
            assert_eq!(row.type_.to_string(), "i64");
            assert!(!row.is_primary_key)
        }

        #[test]
        fn test_row_parse_optional() {
            let row_tokens = TokenStream::from(quote! {id: i64 optional});
            let iter = &mut row_tokens.clone().into_iter();

            let mut parser = Parser::new(row_tokens);
            let row = parser.parse_row();

            assert!(row.is_ok());
            let row = row.unwrap();

            assert_eq!(row.name.to_string(), "id");
            assert_eq!(row.type_.to_string(), "i64");
            assert!(row.optional);
            assert!(!row.is_primary_key)
        }
    }
}
