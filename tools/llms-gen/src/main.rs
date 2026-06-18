//! Generator for the repo-root `llms.txt`.
//!
//! It reads the curated `docs/llms.template.md`, replaces the block between the
//! `<!-- BEGIN API -->` and `<!-- END API -->` markers with a sorted inventory of the
//! crate's public items (parsed from `src/lib.rs`, where every public item is re-exported),
//! and writes `llms.txt` at the repo root.
//!
//! The output is deterministic: the same `src/lib.rs` + template always produce a
//! byte-identical `llms.txt`, so the `just check-llms` CI gate can fail when the public API
//! changes but `llms.txt` was not regenerated. Parsing the source text (rather than the
//! compiled crate) means feature-gated items are captured without `--all-features` or a
//! nightly toolchain.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use syn::punctuated::Punctuated;
use syn::{Item, Meta, Token, UseTree, Visibility};

const BEGIN: &str = "<!-- BEGIN API -->";
const END: &str = "<!-- END API -->";

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("tools/llms-gen lives two levels below the repo root")
        .to_path_buf();

    let lib_rs = root.join("src/lib.rs");
    let template = root.join("docs/llms.template.md");
    let output = root.join("llms.txt");

    let source = std::fs::read_to_string(&lib_rs)
        .unwrap_or_else(|e| panic!("read {}: {e}", lib_rs.display()));
    let api = public_api(&source);

    let tmpl = std::fs::read_to_string(&template)
        .unwrap_or_else(|e| panic!("read {}: {e}", template.display()));
    let rendered = render(&tmpl, &api);

    std::fs::write(&output, rendered).unwrap_or_else(|e| panic!("write {}: {e}", output.display()));
    eprintln!("wrote {} ({} public items)", output.display(), api.len());
}

/// Parse `src/lib.rs` and return a sorted map of `public item name -> required features`.
fn public_api(source: &str) -> BTreeMap<String, BTreeSet<String>> {
    let file = syn::parse_file(source).expect("parse src/lib.rs");
    let mut api: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for item in &file.items {
        let (attrs, vis, names): (&[syn::Attribute], &Visibility, Vec<String>) = match item {
            Item::Use(u) => {
                let mut names = Vec::new();
                collect_use_names(&u.tree, &mut names);
                (&u.attrs, &u.vis, names)
            }
            Item::Type(t) => (&t.attrs, &t.vis, vec![t.ident.to_string()]),
            Item::Struct(s) => (&s.attrs, &s.vis, vec![s.ident.to_string()]),
            Item::Enum(e) => (&e.attrs, &e.vis, vec![e.ident.to_string()]),
            Item::Trait(t) => (&t.attrs, &t.vis, vec![t.ident.to_string()]),
            Item::Fn(f) => (&f.attrs, &f.vis, vec![f.sig.ident.to_string()]),
            Item::Const(c) => (&c.attrs, &c.vis, vec![c.ident.to_string()]),
            Item::Static(s) => (&s.attrs, &s.vis, vec![s.ident.to_string()]),
            _ => continue,
        };

        if !matches!(vis, Visibility::Public(_)) {
            continue;
        }
        let Some(features) = cfg_features(attrs) else {
            continue; // `#[cfg(test)]`-gated: not part of the public API
        };
        for name in names {
            api.entry(name)
                .or_default()
                .extend(features.iter().cloned());
        }
    }
    api
}

/// Collect the leaf names a `use` tree brings into scope (what a downstream crate imports).
fn collect_use_names(
    tree: &UseTree,
    out: &mut Vec<String>,
) {
    match tree {
        UseTree::Path(p) => collect_use_names(&p.tree, out),
        UseTree::Name(n) => out.push(n.ident.to_string()),
        UseTree::Rename(r) => out.push(r.rename.to_string()),
        UseTree::Group(g) => g.items.iter().for_each(|t| collect_use_names(t, out)),
        UseTree::Glob(_) => panic!(
            "glob re-export (`pub use …::*`) in src/lib.rs is not supported by the llms.txt \
             generator — list the items explicitly, or extend tools/llms-gen to expand globs"
        ),
    }
}

/// The cargo features required to compile an item, read from its `#[cfg(...)]` attributes.
/// Returns `None` when the item is `#[cfg(test)]`-gated (and therefore not public API).
fn cfg_features(attrs: &[syn::Attribute]) -> Option<BTreeSet<String>> {
    let mut features = BTreeSet::new();
    for attr in attrs {
        if !attr.path().is_ident("cfg") {
            continue;
        }
        if let Ok(meta) = attr.parse_args::<Meta>() {
            let mut has_test = false;
            walk_cfg(&meta, &mut features, &mut has_test);
            if has_test {
                return None;
            }
        }
    }
    Some(features)
}

/// Walk a `cfg` predicate, collecting every `feature = "…"` and noting any `test` flag.
fn walk_cfg(
    meta: &Meta,
    features: &mut BTreeSet<String>,
    has_test: &mut bool,
) {
    match meta {
        Meta::Path(p) if p.is_ident("test") => *has_test = true,
        Meta::Path(_) => {}
        Meta::NameValue(nv) if nv.path.is_ident("feature") => {
            if let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(s),
                ..
            }) = &nv.value
            {
                features.insert(s.value());
            }
        }
        Meta::NameValue(_) => {}
        // Only `all(...)` matches the renderer's "requires every feature" semantics.
        Meta::List(list) if list.path.is_ident("all") => {
            if let Ok(nested) =
                list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            {
                for m in &nested {
                    walk_cfg(m, features, has_test);
                }
            }
        }
        Meta::List(_) => panic!(
            "unsupported cfg predicate on a public re-export in src/lib.rs: the llms.txt \
             generator handles only `feature = \"…\"` and `all(...)`; extend tools/llms-gen \
             to support `any`/`not`"
        ),
    }
}

/// Render the template, replacing the `BEGIN..END` block with the API inventory.
fn render(
    template: &str,
    api: &BTreeMap<String, BTreeSet<String>>,
) -> String {
    let begin = template.find(BEGIN).expect("template missing BEGIN marker");
    let end = template.find(END).expect("template missing END marker");
    assert!(begin < end, "BEGIN marker must precede END marker");

    let mut body = String::new();
    for (name, feats) in api {
        if feats.is_empty() {
            body.push_str(&format!("- `{name}`\n"));
        } else {
            let list = feats
                .iter()
                .map(|f| format!("`{f}`"))
                .collect::<Vec<_>>()
                .join(" + ");
            body.push_str(&format!("- `{name}` — requires {list}\n"));
        }
    }

    format!(
        "{}\n{}{}",
        &template[..begin + BEGIN.len()],
        body,
        &template[end..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
mod private;
pub type Alias = u8;
pub use private::{A, B as Renamed};
#[cfg(feature = "x")]
pub use private::C;
#[cfg(all(feature = "x", feature = "y"))]
pub use private::D;
#[cfg(test)]
pub use private::TestOnly;
pub(crate) use private::NotPublic;
"#;

    #[test]
    fn extracts_public_items_with_features_and_skips_non_public() {
        let api = public_api(SAMPLE);
        let names: Vec<&str> = api.keys().map(String::as_str).collect();
        // Sorted (BTreeMap), renames use the new name, `pub(crate)` and `#[cfg(test)]` excluded.
        assert_eq!(names, vec!["A", "Alias", "C", "D", "Renamed"]);
        assert!(api["A"].is_empty());
        assert_eq!(api["C"].iter().cloned().collect::<Vec<_>>(), vec!["x"]);
        assert_eq!(api["D"].iter().cloned().collect::<Vec<_>>(), vec!["x", "y"]);
    }

    #[test]
    fn generation_is_deterministic() {
        assert_eq!(public_api(SAMPLE), public_api(SAMPLE));
    }

    #[test]
    fn render_replaces_only_between_markers() {
        let template = "head\n<!-- BEGIN API -->\nstale\n<!-- END API -->\ntail\n";
        let api = public_api("pub use m::Only;");
        let out = render(template, &api);
        assert!(out.starts_with("head\n<!-- BEGIN API -->\n"));
        assert!(out.ends_with("<!-- END API -->\ntail\n"));
        assert!(out.contains("- `Only`\n"));
        assert!(!out.contains("stale"));
    }

    #[test]
    fn render_annotates_required_features() {
        let api =
            public_api("#[cfg(all(feature = \"serde\", feature = \"tokio\"))]\npub use m::T;");
        let out = render("<!-- BEGIN API -->\n<!-- END API -->", &api);
        assert!(out.contains("- `T` — requires `serde` + `tokio`\n"));
    }

    #[test]
    #[should_panic(expected = "glob re-export")]
    fn rejects_glob_reexport() {
        public_api("pub use m::*;");
    }

    #[test]
    #[should_panic(expected = "unsupported cfg predicate")]
    fn rejects_any_cfg() {
        public_api("#[cfg(any(feature = \"a\", feature = \"b\"))]\npub use m::T;");
    }
}
