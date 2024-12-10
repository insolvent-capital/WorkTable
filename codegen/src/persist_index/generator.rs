use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::{quote, ToTokens};
use syn::ItemStruct;

use std::collections::HashMap;

pub struct Generator {
    struct_def: ItemStruct,
    field_types: HashMap<Ident, TokenStream>,
}

impl Generator {
    pub fn new(struct_def: ItemStruct) -> Self {
        Self {
            struct_def,
            field_types: HashMap::new(),
        }
    }

    pub fn gen_persist_type(&mut self) -> syn::Result<TokenStream> {
        let name_ident = Ident::new(
            format!("{}Persisted", self.struct_def.ident).as_str(),
            Span::mixed_site(),
        );
        let mut fields = vec![];
        let mut types = vec![];

        for field in &self.struct_def.fields {
            fields.push(field.ident.clone().unwrap());
            let index_type = field.ty.to_token_stream().to_string();
            let mut split = index_type.split("<");
            // skip `TreeIndex`
            split.next();
            let substr = split.next().unwrap().to_string();
            types.push(substr.split(",").next().unwrap().to_string());
        }

        let fields: Vec<_> = fields
            .into_iter()
            .zip(types)
            .map(|(i, t)| {
                let t: TokenStream = t.parse().unwrap();
                self.field_types.insert(i.clone(), t.clone());
                quote! {
                    #i: Vec<GeneralPage<IndexData<#t>>>,
                }
            })
            .collect();

        Ok(quote! {
            #[derive(Debug, Default, Clone)]
            pub struct #name_ident {
                #(#fields)*
            }
        })
    }

    pub fn gen_persist_impl(&mut self) -> syn::Result<TokenStream> {
        let name_ident = Ident::new(
            format!("{}Persisted", self.struct_def.ident).as_str(),
            Span::mixed_site(),
        );
        let field_names_list: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                (
                    Literal::string(f.ident.as_ref().unwrap().to_string().as_str()),
                    f.ident.as_ref().unwrap(),
                )
            })
            .map(|(l, i)| {
                quote! {
                    let i = Interval (
                        self.#i.first().unwrap().header.page_id.into(),
                        self.#i.last().unwrap().header.page_id.into()
                    );
                    map.insert(#l.to_string(), vec![i]);
                }
            })
            .collect();
        let last_header: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| f.ident.as_ref().unwrap())
            .map(|i| {
                quote! {
                    if header.is_none() {
                        header = Some(&mut self.#i.last_mut().unwrap().header);
                    } else {
                        let new_header = &mut self.#i.last_mut().unwrap().header;
                        if header.as_ref().unwrap().page_id < new_header.page_id {
                            header = Some(new_header)
                        }
                    }
                }
            })
            .collect();
        let persist_logic = self
            .struct_def
            .fields
            .iter()
            .map(|f| f.ident.as_ref().unwrap())
            .map(|i| {
                quote! {
                    for mut page in &mut self.#i {
                        persist_page(&mut page, file)?;
                    }
                }
            })
            .collect::<Vec<_>>();

        let parse_from_file = self.gen_parse_from_file()?;
        Ok(quote! {
            impl #name_ident {
                pub fn get_intervals(&self) -> std::collections::HashMap<String, Vec<Interval>> {
                    let mut map = std::collections::HashMap::new();

                    #(#field_names_list)*

                    map
                }

                pub fn get_last_header_mut(&mut self) -> Option<&mut GeneralHeader> {
                    let mut header = None;

                    #(#last_header)*

                    header
                }

                pub fn persist(&mut self, file: &mut std::fs::File) -> eyre::Result<()> {
                    #(#persist_logic)*

                    Ok(())
                }

                #parse_from_file
            }
        })
    }

    pub fn gen_parse_from_file(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("Index", "");
        let page_const_name = Ident::new(
            format!("{}_PAGE_SIZE", name.to_uppercase()).as_str(),
            Span::mixed_site(),
        );
        let field_names_lits: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| (Literal::string(f.ident.as_ref().unwrap().to_string().as_str()), f.ident.as_ref().unwrap()))
            .map(|(l, i)| quote! {
                let mut #i = vec![];
                let intervals = map.get(#l).expect("exists");
                for interval in intervals {
                    for page_id in interval.0..interval.1 {
                        let index = parse_page::<IndexData<_>, { #page_const_name as u32 }>(file, page_id as u32)?;
                        #i.push(index);
                    }
                    let index = parse_page::<IndexData<_>, { #page_const_name as u32 }>(file, interval.1 as u32)?;
                    #i.push(index);
                }
            })
            .collect();
        let idents = self
            .struct_def
            .fields
            .iter()
            .map(|f| f.ident.as_ref().unwrap())
            .collect::<Vec<_>>();

        Ok(quote! {
            pub fn parse_from_file(file: &mut std::fs::File, map: &std::collections::HashMap<String, Vec<Interval>>) -> eyre::Result<Self> {
                #(#field_names_lits)*

                Ok(Self {
                    #(#idents,)*
                })
            }
        })
    }

    pub fn gen_persistable_impl(&self) -> syn::Result<TokenStream> {
        let ident = &self.struct_def.ident;
        let name_ident = Ident::new(
            format!("{}Persisted", self.struct_def.ident).as_str(),
            Span::mixed_site(),
        );
        let field_names_lits: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| Literal::string(f.ident.as_ref().unwrap().to_string().as_str()))
            .map(|l| quote! { #l, })
            .collect();
        let persisted_index_fn = self.gen_persisted_index_fn()?;
        let from_persisted_fn = self.gen_from_persisted_fn()?;

        Ok(quote! {
            impl PersistableIndex for #ident {
                type PersistedIndex = #name_ident;

                fn get_index_names(&self) -> Vec<&str> {
                    vec![#(#field_names_lits)*]
                }

                #persisted_index_fn
                #from_persisted_fn
            }
        })
    }

    fn gen_persisted_index_fn(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("Index", "");
        let const_name = Ident::new(
            format!("{}_PAGE_SIZE", name.to_uppercase()).as_str(),
            Span::mixed_site(),
        );
        let idents = self
            .struct_def
            .fields
            .iter()
            .map(|f| f.ident.as_ref().unwrap())
            .collect::<Vec<_>>();
        let field_names_init: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                (
                    f.ident.as_ref().unwrap(),
                    !f.ty
                        .to_token_stream()
                        .to_string()
                        .to_lowercase()
                        .contains("lockfree"),
                )
            })
            .map(|(i, is_unique)| {
                let ty = self.field_types.get(i).unwrap();
                if is_unique {
                    quote! {
                        let mut #i = map_index_pages_to_general(map_unique_tree_index::<#ty, #const_name>(TableIndex::iter(&self.#i)), previous_header);
                        previous_header = &mut #i.last_mut().unwrap().header;
                    }
                } else {
                    quote! {
                        let mut #i =  map_index_pages_to_general(map_tree_index::<#ty, #const_name>(TableIndex::iter(&self.#i)), previous_header);
                        previous_header = &mut #i.last_mut().unwrap().header;
                    }
                }
            })
            .collect();

        Ok(quote! {
            fn get_persisted_index(&self, header: &mut GeneralHeader) -> Self::PersistedIndex {
                let mut previous_header = header;

                #(#field_names_init)*

                Self::PersistedIndex {
                    #(#idents,)*
                }
            }
        })
    }

    fn gen_from_persisted_fn(&self) -> syn::Result<TokenStream> {
        let idents = self
            .struct_def
            .fields
            .iter()
            .map(|f| f.ident.as_ref().unwrap())
            .collect::<Vec<_>>();
        let index_gen = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                let i = f.ident.as_ref().unwrap();
                let index_type = f.ty.to_token_stream().to_string();
                let mut split = index_type.split("<");
                let t = Ident::new(split.next().unwrap().trim(), Span::mixed_site());

                let is_unique = !f
                    .ty
                    .to_token_stream()
                    .to_string()
                    .to_lowercase()
                    .contains("lockfree");
                if is_unique {
                    quote! {
                        let #i: #t<_, Link> = #t::new();
                        for page in persisted.#i {
                            for val in page.inner.index_values {
                                TableIndex::insert(&#i, val.key, val.link)
                                    .expect("index is unique");
                            }
                        }
                    }
                } else {
                    quote! {
                        let #i: #t<_, std::sync::Arc<lockfree::set::Set<Link>>> = #t::new();
                        for page in persisted.#i {
                            for val in page.inner.index_values {
                                if let Some(set) = TableIndex::peek(&#i, &val.key) {
                                    set.insert(val.link).expect("is ok");
                                } else {
                                    let set = lockfree::set::Set::new();
                                    set.insert(val.link).expect("is ok");
                                    TableIndex::insert(&#i, val.key, std::sync::Arc::new(set))
                                        .expect("index is unique");
                                }
                            }
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        Ok(quote! {
            fn from_persisted(persisted: Self::PersistedIndex) -> Self {
                #(#index_gen)*

                Self {
                    #(#idents,)*
                }
            }
        })
    }
}
