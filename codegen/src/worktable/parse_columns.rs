use proc_macro2::token_stream;
use proc_macro2::{Delimiter, Ident, TokenStream, TokenTree};
use std::collections::HashMap;
use std::iter::Peekable;
use syn::spanned::Spanned as _;

use crate::worktable::parse_punct::{parse_colon, try_parse_comma};

#[derive(Debug, Clone)]
pub struct Columns {
    pub columns_map: HashMap<Ident, Ident>,
    pub indexes: Vec<Ident>,
    pub primary_key: Ident,
}

impl Columns {
    fn try_from_rows(rows: Vec<Row>, input: &TokenStream) -> syn::Result<Self> {
        let mut columns_map = HashMap::new();
        let mut pk = None;
        let mut indexes = Vec::new();

        for row in rows {
            columns_map.insert(row.name.clone(), row.type_.clone());

            match row.index_flag {
                1 => {
                    if let Some(_) = pk {
                        return Err(syn::Error::new(
                            input.span(),
                            "Only one primary key column allowed",
                        ));
                    } else {
                        pk = Some(row.name)
                    }
                },
                2 => {
                    indexes.push(row.name)
                }
                0 => {}
                _ => unreachable!()
            }
        }

        if pk.is_none() {
            return Err(syn::Error::new(input.span(), "Primary key must be set"));
        }

        Ok(Self {
            columns_map,
            indexes,
            primary_key: pk.expect("checked before"),
        })
    }
}

#[derive(Debug)]
struct Row {
    name: Ident,
    type_: Ident,
    index_flag: u8,
}

pub fn parse_columns(
    iter: &mut token_stream::IntoIter,
    input: &TokenStream,
) -> syn::Result<Columns> {
    let ident = iter.next().ok_or(syn::Error::new(
        input.span(),
        "Expected `columns` field in declaration",
    ))?;
    if let TokenTree::Ident(ident) = ident {
        if ident.to_string().as_str() != "columns" {
            return Err(syn::Error::new(
                input.span(),
                "Expected `columns` field. `WorkTable` name must be specified",
            ));
        }
    } else {
        return Err(syn::Error::new(
            input.span(),
            "Expected field name identifier.",
        ));
    };

    parse_colon(iter, input)?;

    let tt = {
        let group = iter.next().ok_or(syn::Error::new(
            input.span(),
            "Expected `columns` declarations",
        ))?;
        if let TokenTree::Group(group) = group {
            if group.delimiter() != Delimiter::Brace {
                return Err(syn::Error::new(input.span(), "Expected brace"));
            }
            group.stream()
        } else {
            return Err(syn::Error::new(
                input.span(),
                "Expected `columns` declarations",
            ));
        }
    };
    let mut iter = tt.into_iter();
    let iter = &mut iter;

    let mut rows = Vec::new();
    let mut ind = true;
    let mut iter = iter.peekable();

    while ind {
        let row = parse_row(&mut iter, input)?;
        rows.push(row);
        ind = iter.peek().is_some()
    }

    Columns::try_from_rows(rows, input)
}

fn parse_row(
    mut iter: &mut Peekable<&mut impl Iterator<Item = TokenTree>>,
    input: &TokenStream,
) -> syn::Result<Row> {
    let ident = iter.next().ok_or(syn::Error::new(
        input.span(),
        "Expected column name in declaration",
    ))?;
    println!("{ident}");
    let name = if let TokenTree::Ident(ident) = ident {
        ident
    } else {
        return Err(syn::Error::new(input.span(), "Expected identifier."));
    };

    parse_colon(iter, input)?;

    let type_ = iter.next().ok_or(syn::Error::new(
        input.span(),
        "Expected column type in declaration",
    ))?;
    let type_ = if let TokenTree::Ident(type_) = type_ {
        type_
    } else {
        return Err(syn::Error::new(input.span(), "Expected type."));
    };

    let index_flag = if let Some(tt) = iter.peek() {
        match tt {
            TokenTree::Ident(index) => {
                if index.to_string().as_str() == "primary_key" {
                    iter.next();
                    1
                } else if  index.to_string().as_str() == "index" {
                    iter.next();
                    2
                } else {
                    return Err(syn::Error::new(input.span(), "Unexpected identifier."));
                }
            }
            TokenTree::Punct(comma) => {
                if comma.as_char() != ',' {
                    return Err(syn::Error::new(
                        input.span(),
                        format!("Expected `,` found: `{}`", comma.as_char()),
                    ));
                }
                iter.next();
                0
            }
            _ => 0,
        }
    } else {
        0
    };

    try_parse_comma(&mut iter, input)?;

    Ok(Row {
        name,
        type_,
        index_flag,
    })
}

#[cfg(test)]
mod tests {
    use proc_macro2::TokenStream;
    use quote::quote;
    use std::collections::HashMap;

    use super::{parse_columns, parse_row};

    #[test]
    fn test_columns_parse() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64 primary_key,
            test: u64,
        }});

        let columns = parse_columns(&mut tokens.clone().into_iter(), &tokens);

        assert!(columns.is_ok());
        let columns = columns.unwrap();

        assert_eq!(columns.primary_key.to_string(), "id");

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

        let columns = parse_columns(&mut tokens.clone().into_iter(), &tokens);

        assert!(columns.is_ok());
        let columns = columns.unwrap();

        assert_eq!(columns.primary_key.to_string(), "id");

        let map: HashMap<_, _> = columns
            .columns_map
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(map.get("id"), Some(&"i64".to_string()));
        assert_eq!(map.get("test"), Some(&"u64".to_string()));
    }

    #[test]
    fn test_columns_parse_three() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64 primary_key,
            test: u64 index,
            a: u64
        }});

        let columns = parse_columns(&mut tokens.clone().into_iter(), &tokens);

        let columns = columns.unwrap();

        assert_eq!(columns.primary_key.to_string(), "id");
        assert_eq!(columns.indexes.into_iter().map(|v| v.to_string()).collect::<Vec<_>>(), vec!["test"]);

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

        let columns = parse_columns(&mut tokens.clone().into_iter(), &tokens);

        assert!(columns.is_err());
    }

    mod row {
        use super::*;

        #[test]
        fn test_row_parse() {
            let row_tokens = TokenStream::from(quote! {id: i64 primary_key,});
            let iter = &mut row_tokens.clone().into_iter();

            let row = parse_row(&mut iter.peekable(), &row_tokens);

            assert!(row.is_ok());
            let row = row.unwrap();

            assert_eq!(row.name.to_string(), "id");
            assert_eq!(row.type_.to_string(), "i64");
            assert_eq!(row.index_flag, 0)
        }

        #[test]
        fn test_row_parse_no_comma() {
            let row_tokens = TokenStream::from(quote! {id: i64 primary_key});
            let iter = &mut row_tokens.clone().into_iter();

            let row = parse_row(&mut iter.peekable(), &row_tokens);

            assert!(row.is_ok());
            let row = row.unwrap();

            assert_eq!(row.name.to_string(), "id");
            assert_eq!(row.type_.to_string(), "i64");
            assert_eq!(row.index_flag, 1)
        }

        #[test]
        fn test_row_parse_no_primary_key() {
            let row_tokens = TokenStream::from(quote! {id: i64,});
            let iter = &mut row_tokens.clone().into_iter();

            let row = parse_row(&mut iter.peekable(), &row_tokens);

            assert!(row.is_ok());
            let row = row.unwrap();

            assert_eq!(row.name.to_string(), "id");
            assert_eq!(row.type_.to_string(), "i64");
            assert_eq!(row.index_flag, 0)
        }

        #[test]
        fn test_row_parse_no_primary_key_no_comma() {
            let row_tokens = TokenStream::from(quote! {id: i64});
            let iter = &mut row_tokens.clone().into_iter();

            let row = parse_row(&mut iter.peekable(), &row_tokens);

            assert!(row.is_ok());
            let row = row.unwrap();

            assert_eq!(row.name.to_string(), "id");
            assert_eq!(row.type_.to_string(), "i64");
            assert_eq!(row.index_flag, 0)
        }
    }
}
