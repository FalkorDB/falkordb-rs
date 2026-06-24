{#-
  code_file - embed a real, CI-compiled source file (or an // ANCHOR: region of one).

  The file is read from blog/snippets/, which `just blog-sync` fills with byte-for-byte
  copies of examples/*.rs and benches/*.rs. Those same files are compiled by
  `just build-examples` / the benches build in CI, so what the reader sees is exactly what
  CI built. We fence with ~~~ (not backticks), because Rust sources embed backtick fences in
  their doc comments.

  NOTE: the highlight param is `language`, NOT `lang` - `lang` is a reserved Zola variable
  (the page language) and would override the fence with "en".

  Params:
    file     - canonical repo path, e.g. "examples/typed_params.rs" (required; used for the link)
    anchor   - optional ANCHOR name; loads just that region instead of the whole file
    language - language for highlighting (default: rust)
-#}
{%- set fence_lang = language | default(value="rust") -%}
{%- set slug = file | replace(from="/", to="_") | replace(from=".", to="_") -%}
{%- if anchor -%}{%- set load_path = "snippets/anchors/" ~ slug ~ "__" ~ anchor ~ ".rs" -%}{%- else -%}{%- set load_path = "snippets/" ~ file -%}{%- endif -%}
{%- set content = load_data(path=load_path, format="plain") -%}
{%- set link = config.extra.repo_url ~ "/blob/" ~ config.extra.source_ref ~ "/" ~ file -%}
~~~{{ fence_lang }}
{{ content | trim }}
~~~
<p class="source-note">{% if anchor %}Excerpt from{% else %}Source:{% endif %} <a href="{{ link }}"><code>{{ file }}</code></a> &mdash; compiled in CI{% if anchor %}; the full file builds, this is the <code>{{ anchor }}</code> region{% endif %}.</p>
