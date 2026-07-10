"""Decision-core codegen rules: Decision.tla → Go *spec + optional Rust via //tools/specgen."""

def decision_core(
        name,
        tla,
        dest,
        pkg,
        consts,
        src_go,
        src_test,
        src_rs = None):
    """Declare genrule + up_to_date tests for one decision core.

    Args:
      name: Bazel-safe name (underscores), e.g. "timing_clamp".
      tla: Label of Decision.tla.
      dest: Source-tree Go package path (for docs / update.sh).
      pkg: Go package / Rust module hint for -p.
      consts: List of "Name=Value" for -const flags.
      src_go / src_test: Labels of committed generated Go sources.
      src_rs: Optional label of committed Rust lib (crates/.../src/<mod>.rs).
    """
    _ = dest  # reserved for update.sh mapping docs

    const_flags = " ".join(["-const " + c for c in consts])
    gen_name = name + "_gen"

    outs = [
        name + "_gen/spec.go",
        name + "_gen/spec_test.go",
        name + "_gen/spec.rs",
    ]

    native.genrule(
        name = gen_name,
        srcs = [tla],
        outs = outs,
        cmd = """
set -euo pipefail
tmpdir=codegen_tmp_{name}
mkdir -p "$$tmpdir"
# Go (production ote path)
$(location //tools/specgen) {consts} -o "$$tmpdir" -p {pkg} $(location {tla})
for f in "$$tmpdir/spec.go" "$$tmpdir/spec_test.go"; do
  sed -e 's|^// Regenerate:.*|// Regenerate: bazel run //tools/decision:update|' "$$f" > "$$f.sed"
  mv "$$f.sed" "$$f"
done
# Rust peer (same Decision.tla SSOT)
$(location //tools/specgen) -lang rust {consts} -o "$$tmpdir" -p {pkg} $(location {tla})
sed -e 's|^// Regenerate:.*|// Regenerate: bazel run //tools/decision:update|' "$$tmpdir/spec.rs" > "$$tmpdir/spec.rs.sed"
mv "$$tmpdir/spec.rs.sed" "$$tmpdir/spec.rs"
cp "$$tmpdir/spec.go" $(location {name}_gen/spec.go)
cp "$$tmpdir/spec_test.go" $(location {name}_gen/spec_test.go)
cp "$$tmpdir/spec.rs" $(location {name}_gen/spec.rs)
rm -rf "$$tmpdir"
""".format(
            consts = const_flags,
            pkg = pkg,
            tla = tla,
            name = name,
        ),
        tools = ["//tools/specgen"],
        visibility = ["//visibility:public"],
    )

    gen_go = ":" + name + "_gen/spec.go"
    gen_test = ":" + name + "_gen/spec_test.go"
    gen_rs = ":" + name + "_gen/spec.rs"
    native.sh_test(
        name = name + "_up_to_date",
        srcs = ["diff_gen.sh"],
        args = [
            "$(location {gen_go})".format(gen_go = gen_go),
            "$(location {gen_test})".format(gen_test = gen_test),
            "$(location {src_go})".format(src_go = src_go),
            "$(location {src_test})".format(src_test = src_test),
        ],
        data = [
            gen_go,
            gen_test,
            src_go,
            src_test,
        ],
        size = "small",
        tags = ["decision"],
    )

    if src_rs != None:
        native.sh_test(
            name = name + "_rs_up_to_date",
            srcs = ["diff_rs.sh"],
            args = [
                "$(location {gen_rs})".format(gen_rs = gen_rs),
                "$(location {src_rs})".format(src_rs = src_rs),
            ],
            data = [gen_rs, src_rs],
            size = "small",
            tags = ["decision", "rust"],
        )
