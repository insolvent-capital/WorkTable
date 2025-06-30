use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Result, Type};

fn gen_heap_size_body(data: &Data) -> Result<TokenStream> {
    gen_mem_fn_body(
        data,
        quote! { heap_size() },
        quote! { std::mem::size_of::<Self>() },
    )
}

fn gen_used_size_body(data: &Data) -> Result<TokenStream> {
    gen_mem_fn_body(
        data,
        quote! { used_size() },
        quote! { std::mem::size_of::<Self>() },
    )
}

fn gen_mem_fn_body(
    data: &Data,
    method: TokenStream,
    default_for_copy: TokenStream,
) -> Result<TokenStream> {
    match data {
        Data::Struct(data_struct) => {
            let fields = match &data_struct.fields {
                Fields::Named(named) => named.named.iter().collect::<Vec<_>>(),
                Fields::Unnamed(unnamed) => unnamed.unnamed.iter().collect::<Vec<_>>(),
                Fields::Unit => vec![],
            };

            if fields.is_empty() {
                Ok(quote! { 0 })
            } else if fields.iter().all(|f| is_copy_primitive(&f.ty)) {
                Ok(default_for_copy)
            } else {
                let field_sizes = fields.iter().enumerate().map(|(i, f)| {
                    let accessor = match &f.ident {
                        Some(ident) => quote! { self.#ident },
                        None => {
                            let index = syn::Index::from(i);
                            quote! { self.#index }
                        }
                    };
                    quote! { size += #accessor.#method; }
                });

                Ok(quote! {
                    let mut size = 0;
                    #(#field_sizes)*
                    size
                })
            }
        }

        Data::Enum(enum_data) => {
            let arms = enum_data.variants.iter().map(|variant| {
                let name = &variant.ident;
                match &variant.fields {
                    Fields::Unit => {
                        quote! {
                            Self::#name => 0,
                        }
                    }
                    Fields::Unnamed(fields) => {
                        let bindings: Vec<_> = (0..fields.unnamed.len())
                            .map(|i| syn::Ident::new(&format!("f{i}"), variant.ident.span()))
                            .collect();

                        let calls = bindings
                            .iter()
                            .map(|b| quote! { #b.#method })
                            .collect::<Vec<_>>();
                        quote! {
                            Self::#name(#(#bindings),*) => {
                                0 #(+ #calls)*
                            },
                        }
                    }
                    Fields::Named(fields) => {
                        let bindings: Vec<_> = fields
                            .named
                            .iter()
                            .map(|f| f.ident.as_ref().unwrap())
                            .collect();

                        let calls = bindings
                            .iter()
                            .map(|b| quote! { #b.#method })
                            .collect::<Vec<_>>();
                        quote! {
                            Self::#name { #(#bindings),* } => {
                                0 #(+ #calls)*
                            },
                        }
                    }
                }
            });

            Ok(quote! {
                match self {
                    #(#arms)*
                }
            })
        }

        _ => Err(syn::Error::new_spanned(
            method,
            "#[derive(MemStat)] only supports structs and enums",
        )),
    }
}

pub fn expand(input: proc_macro2::TokenStream) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let name = &input.ident;

    let heap = gen_heap_size_body(&input.data)?;
    let used = gen_used_size_body(&input.data)?;

    Ok(quote! {
        impl MemStat for #name {
            fn heap_size(&self) -> usize {
                #heap
            }
            fn used_size(&self) -> usize {
                #used
            }
        }
    })
}

fn is_copy_primitive(ty: &Type) -> bool {
    matches!(
        ty,
        Type::Path(type_path)
            if type_path.qself.is_none() &&
               type_path.path.segments.len() == 1 &&
               matches!(
                   type_path.path.segments[0].ident.to_string().as_str(),
                   "u8" | "u16" | "u32" | "u64" | "usize" |
                   "i8" | "i16" | "i32" | "i64" | "isize" |
                   "bool" | "char" | "f64" | "f32"
               )
    )
}
