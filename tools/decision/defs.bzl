"""Decision-core codegen rules: Decision.tla → pkg/.../*spec via //tools/specgen."""

def decision_core(
        name,
        tla,
        dest,
        pkg,
        consts,
        src_go,
        src_test):
    """Declare genrule + up_to_date test for one decision core.

    Args:
      name: Bazel-safe name (underscores), e.g. "timing_clamp".
      tla: Label of Decision.tla.
      dest: Source-tree package path (for docs only).
      pkg: Go package name for -p.
      consts: List of "Name=Value" for -const flags.
      src_go / src_test: Labels of committed generated sources.
    """
    _ = dest  # reserved for update.sh mapping docs

    const_flags = " ".join(["-const " + c for c in consts])
    gen_name = name + "_gen"

    native.genrule(
        name = gen_name,
        srcs = [tla],
        outs = [
            name + "_gen/spec.go",
            name + "_gen/spec_test.go",
        ],
        cmd = """
set -euo pipefail
# Relative out dir so headers stay path-stable; then rewrite Regenerate line
# to the Bazel update target (canonical for this repo).
tmpdir=codegen_tmp_{name}
mkdir -p "$$tmpdir"
$(location //tools/specgen) {consts} -o "$$tmpdir" -p {pkg} $(location {tla})
for f in "$$tmpdir/spec.go" "$$tmpdir/spec_test.go"; do
  # portable sed: rewrite regenerate instruction to the Bazel entrypoint
  sed -e 's|^// Regenerate:.*|// Regenerate: bazel run //tools/decision:update|' "$$f" > "$$f.sed"
  mv "$$f.sed" "$$f"
done
cp "$$tmpdir/spec.go" $(location {name}_gen/spec.go)
cp "$$tmpdir/spec_test.go" $(location {name}_gen/spec_test.go)
rm -rf "$$tmpdir"
""".format(
            consts = const_flags,
            pkg = pkg,
            tla = tla,
            name = name,
        ),
        tools = ["//tools/specgen"],
        # Public so go_library in pkg/.../*spec can use outs as srcs
        # (complete graph: Decision.tla → genrule → go_library → ote).
        visibility = ["//visibility:public"],
    )

    gen_go = ":" + name + "_gen/spec.go"
    gen_test = ":" + name + "_gen/spec_test.go"
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
