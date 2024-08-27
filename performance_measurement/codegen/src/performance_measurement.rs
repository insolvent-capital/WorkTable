
use proc_macro2::{Ident, Literal, token_stream, TokenStream, TokenTree};
use quote::quote;
use syn::spanned::Spanned;

#[derive(Debug)]
pub struct PerformanceMeasurementAttr {
    name: String,
}

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let input_fn = match syn::parse2::<syn::ItemFn>(item.clone()) {
        Ok(data) => data,
        Err(err) => {
            return Err(syn::Error::new(item.span(), err.to_string()));
        }
    };

    let fn_name = &input_fn.sig.ident;
    let fn_block = &input_fn.block;

    let fn_sig = &input_fn.sig;

    let mut attr = parse_attr(attr)?;

    let mut test_sig = input_fn.sig.clone();
    test_sig.ident = Ident::new(format!("__{}", test_sig.ident).as_str(), item.span());


    let fn_name = Literal::string(format!("{}::{}", attr.name, fn_name.to_string()).as_str());

    Ok(quote! {
        #fn_sig {
            let start = performance_measurement::PerformanceProfiler::get_now();
            let res = #fn_block;
            performance_measurement::PerformanceProfiler::store_measurement(#fn_name, start.elapsed());

            res
        }
    })
}

pub fn parse_attr(attr: TokenStream) -> syn::Result<PerformanceMeasurementAttr> {
    let mut i = attr.clone().into_iter();

    let name = parse_name(&mut i, &attr)?;

    Ok(PerformanceMeasurementAttr {
        name
    })
}

pub fn parse_name(iter: &mut token_stream::IntoIter, attr: &TokenStream) -> syn::Result<String> {
    let name_field = iter.next().ok_or(syn::Error::new(attr.span(), "Expected `prefix_name` field"))?;
    if let TokenTree::Ident(field) = name_field {
        if field.to_string() != "prefix_name".to_string() {
            return Err(syn::Error::new(attr.span(), "Expected `prefix_name` field"))
        };
    } else {
        return Err(syn::Error::new(attr.span(), "Expected `prefix_name` field"))
    }

    let eq = iter.next().ok_or(syn::Error::new(attr.span(), "Expected `=`"))?;
    if let TokenTree::Punct(eq) = eq {
        if eq.to_string() != "=".to_string() {
            return Err(syn::Error::new(attr.span(), "Expected `=`"))
        };
    } else {
        return Err(syn::Error::new(attr.span(), "Expected `=`"))
    }

    let name = iter.next().ok_or(syn::Error::new(attr.span(), "Expected name itself"))?;
    if let TokenTree::Literal(name) = name {
        Ok(name.to_string().replace('"', ""))
    } else {
        Err(syn::Error::new(attr.span(), "Expected `=`"))
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::TokenStream;
    use quote::quote;

    use super::parse_attr;

    #[test]
    fn test_attr_parse() {
        let tokens = TokenStream::from(quote! {prefix_name = "Test"});
        let attr = parse_attr(tokens).unwrap();

        assert_eq!(attr.name, "Test".to_string())
    }
}