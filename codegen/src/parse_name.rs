use proc_macro2::token_stream;
use proc_macro2::{TokenStream, TokenTree};
use syn::spanned::Spanned as _;

use crate::parse_punct::{parse_colon, parse_comma};

pub fn parse_name(iter: &mut token_stream::IntoIter, input: &TokenStream) -> syn::Result<String> {
    let ident = iter.next().ok_or(syn::Error::new(
        input.span(),
        "Expected `name` field in declaration",
    ))?;
    if let TokenTree::Ident(ident) = ident {
        if ident.to_string().as_str() != "name" {
            return Err(syn::Error::new(
                input.span(),
                "Expected `name` field. `WorkTable` name must be specified",
            ));
        }
    } else {
        return Err(syn::Error::new(
            input.span(),
            "Expected field name identifier.",
        ));
    };

    parse_colon(iter, input)?;

    let name = iter
        .next()
        .ok_or(syn::Error::new(input.span(), "Expected token."))?;
    let name = if let TokenTree::Ident(name) = name {
        name.to_string()
    } else {
        return Err(syn::Error::new(input.span(), "Expected identifier."));
    };

    parse_comma(iter, input)?;

    Ok(name)
}

#[cfg(test)]
mod tests {
    use proc_macro2::TokenStream;
    use quote::quote;

    use super::parse_name;

    #[test]
    fn test_name_parse() {
        let tokens = TokenStream::from(quote! {name: TestName,});

        let name = parse_name(&mut tokens.clone().into_iter(), &tokens);

        assert!(name.is_ok());
        let name = name.unwrap();

        assert_eq!(name, "TestName");
    }

    #[test]
    fn test_empty() {
        let tokens = TokenStream::from(quote! {});

        let name = parse_name(&mut tokens.clone().into_iter(), &tokens);

        assert!(name.is_err());
    }

    #[test]
    fn test_literal_field() {
        let tokens = TokenStream::from(quote! {"nme": TestName,});

        let name = parse_name(&mut tokens.clone().into_iter(), &tokens);

        assert!(name.is_err());
    }

    #[test]
    fn test_wrong_field() {
        let tokens = TokenStream::from(quote! {nme: TestName,});

        let name = parse_name(&mut tokens.clone().into_iter(), &tokens);

        assert!(name.is_err());
    }

    #[test]
    fn test_no_comma() {
        let tokens = TokenStream::from(quote! {name: TestName});

        let name = parse_name(&mut tokens.clone().into_iter(), &tokens);

        assert!(name.is_err());
    }
}
