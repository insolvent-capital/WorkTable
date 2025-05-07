use std::collections::HashMap;

use proc_macro2::{Ident, Literal, TokenStream};
use quote::__private::Span;
use quote::{quote, ToTokens};
use syn::ItemStruct;

use crate::name_generator::{is_unsized, WorktableNameGenerator};
use crate::persist_table::WT_INDEX_EXTENSION;

pub struct Generator {
    pub struct_def: ItemStruct,
    pub field_types: HashMap<Ident, TokenStream>,
}

impl WorktableNameGenerator {
    pub fn from_index_ident(index_ident: &Ident) -> Self {
        Self {
            name: index_ident
                .to_string()
                .strip_suffix("Index")
                .expect("index type nae should end on `Index`")
                .to_string(),
        }
    }

    pub fn get_persisted_index_ident(&self) -> Ident {
        Ident::new(
            format!("{}IndexPersisted", self.name).as_str(),
            Span::mixed_site(),
        )
    }

    pub fn get_space_secondary_index_events_ident(&self) -> Ident {
        Ident::new(
            format!("{}SpaceSecondaryIndexEvents", self.name).as_str(),
            Span::mixed_site(),
        )
    }

    pub fn get_space_secondary_index_ident(&self) -> Ident {
        Ident::new(
            format!("{}SpaceSecondaryIndex", self.name).as_str(),
            Span::mixed_site(),
        )
    }
}

impl Generator {
    pub fn new(struct_def: ItemStruct) -> Self {
        let mut fields = vec![];
        let mut types = vec![];

        for field in &struct_def.fields {
            fields.push(
                field
                    .ident
                    .clone()
                    .expect("index fields should always be named fields"),
            );
            let index_type = field.ty.to_token_stream().to_string();
            let mut split = index_type.split("<");
            // skip `IndexMap` ident.
            split.next();
            let substr = split
                .next()
                .expect("index type should always contain key generic")
                .to_string();
            types.push(
                substr
                    .split(",")
                    .next()
                    .expect("index type should always contain key and value generics")
                    .to_string()
                    .parse()
                    .expect("should be valid because parsed from declaration"),
            );
        }
        let map = fields.into_iter().zip(types).collect::<HashMap<_, _>>();

        Self {
            struct_def,
            field_types: map,
        }
    }

    /// Generates persisted index type. This type has same name as index, but with `Persisted` postfix. Field names of
    /// this type are same to index type, and values are `Vec<GeneralPage<IndexPage<T>>>`, where `T` is index key
    /// type.
    pub fn gen_persist_type(&mut self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let name_ident = name_generator.get_persisted_index_ident();

        let fields: Vec<_> = self
            .field_types
            .iter()
            .map(|(i, t)| {
                if is_unsized(&t.to_string()) {
                    let const_size = name_generator.get_page_inner_size_const_ident();
                    quote! {
                        #i: (Vec<GeneralPage<TableOfContentsPage<#t>>>, Vec<GeneralPage<UnsizedIndexPage<#t, {#const_size as u32}>>>),
                    }
                } else {
                    quote! {
                        #i: (Vec<GeneralPage<TableOfContentsPage<#t>>>, Vec<GeneralPage<IndexPage<#t>>>),
                    }
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
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let name_ident = name_generator.get_persisted_index_ident();

        let persist_fn = self.gen_persist_fn();
        let parse_from_file_fn = self.gen_parse_from_file_fn();

        Ok(quote! {
            impl #name_ident {
                #persist_fn
                #parse_from_file_fn
            }
        })
    }

    /// Generates `persist` function for persisted index. It calls `persist_page` function for every page in index.
    fn gen_persist_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let ident = name_generator.get_work_table_ident();
        let index_extension = Literal::string(WT_INDEX_EXTENSION);

        let persist_logic = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                f.ident
                    .as_ref()
                    .expect("index fields should always be named fields")
            })
            .map(|i| {
                let index_name_literal = Literal::string(i.to_string().as_str());
                quote! {
                    {
                        let mut file = tokio::fs::File::create(format!("{}/{}{}", path, #index_name_literal, #index_extension)).await?;
                        let mut info = #ident::space_info_default();
                        info.inner.page_count = self.#i.1.len() as u32 + self.#i.0.len() as u32;
                        persist_page(&mut info, &mut file).await?;
                        for mut page in &mut self.#i.0 {
                            persist_page(&mut page, &mut file).await?;
                        }
                        for mut page in &mut self.#i.1 {
                            persist_page(&mut page, &mut file).await?;
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            pub async fn persist(&mut self, path: &str) -> eyre::Result<()>
            {
                #(#persist_logic)*
                Ok(())
            }
        }
    }

    /// Generates `parse_from_file` function for persisted index. It calls `parse_page` function for every page in each
    /// index interval and collects them into `Vec`'s. Then this `Vec`'s are used to construct persisted index object.
    fn gen_parse_from_file_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let page_const_name = name_generator.get_page_size_const_ident();
        let index_extension = Literal::string(WT_INDEX_EXTENSION);

        let field_names_literals: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| (
                Literal::string(
                    f.ident
                        .as_ref()
                        .expect("index fields should always be named fields")
                        .to_string()
                        .as_str()
                ),
                f.ident
                    .as_ref()
                    .expect("index fields should always be named fields")
            ))
            .map(|(l, i)| quote! {
                let #i = {
                    let mut #i = vec![];
                    let mut file = tokio::fs::File::open(format!("{}/{}{}", path, #l, #index_extension)).await?;
                    let info = parse_page::<SpaceInfoPage<()>, { #page_const_name as u32 }>(&mut file, 0).await?;
                    let file_length = file.metadata().await?.len();
                    let page_id = file_length / (#page_const_name as u64 + GENERAL_HEADER_SIZE as u64) + 1;
                    let next_page_id = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(page_id as u32));
                    let toc = IndexTableOfContents::<_, { #page_const_name as u32 }>::parse_from_file(&mut file, 0.into(), next_page_id.clone()).await?;
                    for page_id in toc.iter().map(|(_, page_id)| page_id) {
                        let index = parse_page::<_, { #page_const_name as u32 }>(&mut file, (*page_id).into()).await?;
                        #i.push(index);
                    }
                    (toc.pages, #i)
                };
            })
            .collect();

        let idents = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                f.ident
                    .as_ref()
                    .expect("index fields should always be named fields")
            })
            .collect::<Vec<_>>();

        quote! {
            pub async fn parse_from_file(path: &str) -> eyre::Result<Self> {
                #(#field_names_literals)*

                Ok(Self {
                    #(#idents,)*
                })
            }
        }
    }

    /// Generates `PersistableIndex` trait implementation for persisted index.
    pub fn gen_persistable_impl(&self) -> syn::Result<TokenStream> {
        let ident = &self.struct_def.ident;
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let name_ident = name_generator.get_persisted_index_ident();

        let get_persisted_index_fn = self.gen_get_persisted_index_fn();
        let from_persisted_fn = self.gen_from_persisted_fn()?;

        Ok(quote! {
            impl PersistableIndex for #ident {
                type PersistedIndex = #name_ident;

                #get_persisted_index_fn
                #from_persisted_fn
            }
        })
    }

    /// Generates `get_persisted_index` function of `PersistableIndex` trait for persisted index. It maps every
    /// `TreeIndex` into `Vec` of `IndexPage`s using `IndexPage::from_nod` function.
    fn gen_get_persisted_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let const_name = name_generator.get_page_inner_size_const_ident();

        let idents = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                f.ident
                    .as_ref()
                    .expect("index fields should always be named fields")
            })
            .collect::<Vec<_>>();
        let field_names_init: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                f.ident
                    .as_ref()
                    .expect("index fields should always be named fields")
            })
            .map(|i| {
                let ty = self
                    .field_types
                    .get(i)
                    .expect("should be available as constructed from same values");
                if is_unsized(&ty.to_string()) {
                    quote! {
                        let mut pages = vec![];
                        for node in self.#i.iter_nodes() {
                            let page = UnsizedIndexPage::from_node(node.lock_arc().as_ref());
                            pages.push(page);
                        }
                        let (toc, pages) = map_unsized_index_pages_to_toc_and_general::<_, { #const_name as u32 }>(pages);
                        let #i = (toc.pages, pages);
                    }
                } else {
                    quote! {
                        let size = get_index_page_size_from_data_length::<#ty>(#const_name);
                        let mut pages = vec![];
                        for node in self.#i.iter_nodes() {
                            let page = IndexPage::from_node(node.lock_arc().as_ref(), size);
                            pages.push(page);
                        }
                        let (toc, pages) = map_index_pages_to_toc_and_general::<_, { #const_name as u32 }>(pages);
                        let #i = (toc.pages, pages);
                    }
                }
            })
            .collect();

        quote! {
            fn get_persisted_index(&self) -> Self::PersistedIndex {
                #(#field_names_init)*
                Self::PersistedIndex {
                    #(#idents,)*
                }
            }
        }
    }

    /// Generates `from_persisted` function of `PersistableIndex` trait for persisted index. It maps every page in
    /// persisted page back to `TreeIndex`
    fn gen_from_persisted_fn(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let const_name = name_generator.get_page_inner_size_const_ident();

        let idents = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                f.ident
                    .as_ref()
                    .expect("index fields should always be named fields")
            })
            .collect::<Vec<_>>();
        let index_gen = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                let i = f
                    .ident
                    .as_ref()
                    .expect("index fields should always be named fields");
                let index_type = f.ty.to_token_stream().to_string();
                let is_unique = !index_type.contains("IndexMultiMap");
                let mut split = index_type.split("<");
                let t = Ident::new(
                    split
                        .next()
                        .expect("index type should always have generics")
                        .trim(),
                    Span::mixed_site(),
                );
                let ty = self
                    .field_types
                    .get(i)
                    .expect("should be available as constructed from same values");

                if is_unsized(&ty.to_string()) {
                    let node = if is_unique {
                        quote! {
                            let node = UnsizedNode::from_inner(page.inner.get_node(), #const_name);
                            #i.attach_node(node);
                        }
                    } else {
                        quote! {
                            let node = UnsizedNode::from_inner(page.inner.get_node().into_iter().map(|p| p.into()).collect(), #const_name);
                            #i.attach_multi_node(node);
                        }
                    };
                    quote! {
                        let #i: #t<_, Link, UnsizedNode<_>> = #t::with_maximum_node_size(#const_name);
                        for page in persisted.#i.1 {
                            #node
                        }
                    }
                } else {
                    let node = if is_unique {
                        quote! {
                            let node = page.inner.get_node();
                            #i.attach_node(node);
                        }
                    } else {
                        quote! {
                            let node = page.inner.get_node();
                            #i.attach_multi_node(node.into_iter().map(|p| p.into()).collect());
                        }
                    };
                    quote! {
                        let size = get_index_page_size_from_data_length::<#ty>(#const_name);
                        let #i: #t<_, Link> = #t::with_maximum_node_size(size);
                        for page in persisted.#i.1 {
                            #node
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

#[cfg(test)]
mod tests {
    use proc_macro2::{Ident, Span};
    use quote::quote;

    use crate::persist_index::generator::Generator;
    use crate::persist_index::parser::Parser;

    #[test]
    fn correctly_collects_fields() {
        let input = quote! {
            #[derive(Debug, Default, Clone)]
            pub struct TestIndex {
                test_idx: TreeIndex<i64, Link>,
                exchnage_idx: TreeMultiIndex<String, Link>
            }
        };
        let struct_ = Parser::parse_struct(input).unwrap();
        let generator = Generator::new(struct_);

        assert_eq!(
            generator
                .field_types
                .get(&Ident::new("test_idx", Span::call_site()))
                .unwrap()
                .to_string()
                .as_str(),
            "i64"
        );
        assert_eq!(
            generator
                .field_types
                .get(&Ident::new("exchnage_idx", Span::call_site()))
                .unwrap()
                .to_string()
                .as_str(),
            "String"
        );
    }
}
