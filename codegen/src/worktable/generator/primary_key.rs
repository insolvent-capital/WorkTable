use std::collections::HashMap;

use crate::worktable::generator::Generator;
use crate::worktable::model::{GeneratorType, PrimaryKey};

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_pk_def(&mut self) -> syn::Result<TokenStream> {
        let name = &self.name;
        let ident = Ident::new(format!("{name}PrimaryKey").as_str(), Span::mixed_site());
        let vals = self
            .columns
            .primary_keys
            .0
            .iter()
            .map(|i| (i.clone(), self.columns.columns_map.get(i).unwrap().clone()))
            .collect::<HashMap<_, _>>();

        let def = if vals.len() == 1 {
            let type_ = vals.values().next().unwrap();
            quote! {
                #[derive(Clone, rkyv::Archive, Debug, rkyv::Deserialize, rkyv::Serialize, From, Eq, Into, PartialEq, PartialOrd, Ord)]
                pub struct #ident(#type_);
            }
        } else {
            let types = vals.values();
            quote! {
                #[derive(Clone, rkyv::Archive, Debug, rkyv::Deserialize, rkyv::Serialize, From, Eq, Into, PartialEq, PartialOrd, Ord)]
                pub struct #ident(#(#types),*);
            }
        };

        let impl_ = match self.columns.generator_type {
            GeneratorType::None => {
                quote! {
                    impl TablePrimaryKey for #ident {
                        type Generator = ();
                    }
                }
            }
            GeneratorType::Autoincrement => {
                let (i, type_) = vals.iter().next().unwrap();
                let gen = Self::gen_from_type(type_, i)?;
                quote! {
                    impl TablePrimaryKey for #ident {
                        type Generator = #gen;
                    }
                }
            }
            GeneratorType::Custom => {
                quote! {}
            }
        };

        self.pk = Some(PrimaryKey { ident, vals });

        Ok(quote! {
            #def
            #impl_
        })
    }

    fn gen_from_type(type_: &TokenStream, i: &Ident) -> syn::Result<TokenStream> {
        Ok(match type_.to_string().as_str() {
            "u8" => quote! { std::sync::atomic::AtomicU8 },
            "u16" => quote! { std::sync::atomic::AtomicU16 },
            "u32" => quote! { std::sync::atomic::AtomicU32 },
            "u64" => quote! { std::sync::atomic::AtomicU64 },
            "i8" => quote! { std::sync::atomic::AtomicI8 },
            "i16" => quote! { std::sync::atomic::AtomicI16 },
            "i32" => quote! { std::sync::atomic::AtomicI32 },
            "i64" => quote! { std::sync::atomic::AtomicI64 },
            _ => {
                return Err(syn::Error::new(
                    i.span(),
                    "Type is not supported for autoincrement",
                ))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::{Ident, Span, TokenStream};
    use quote::quote;

    use crate::worktable::generator::Generator;
    use crate::worktable::Parser;

    #[test]
    fn test_row_generation() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64 primary_key,
            test: u64,
        }});
        let mut parser = Parser::new(tokens);
        let columns = parser.parse_columns().unwrap();

        let ident = Ident::new("Test", Span::call_site());
        let mut generator = Generator::new(ident, false, columns);

        let pk_def = generator.gen_pk_def();

        assert_eq!(generator.pk.unwrap().ident.to_string(), "TestPrimaryKey");
        assert_eq!(
            pk_def.unwrap().to_string(),
            "pub type TestPrimaryKey = i64 ;"
        );
    }

    #[test]
    fn test_row_generation_multiple() {
        let tokens = TokenStream::from(quote! {columns: {
            id: i64 primary_key,
            test: u64 primary_key,
        }});
        let mut parser = Parser::new(tokens);
        let columns = parser.parse_columns().unwrap();

        let ident = Ident::new("Test", Span::call_site());
        let mut generator = Generator::new(ident, false, columns);

        let pk_def = generator.gen_pk_def();

        assert_eq!(generator.pk.unwrap().ident.to_string(), "TestPrimaryKey");
        assert_eq!(
            pk_def.unwrap().to_string(),
            "pub type TestPrimaryKey = (i64 , u64) ;"
        );
    }
}
