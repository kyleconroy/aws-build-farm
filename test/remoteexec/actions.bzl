"""Generates N trivial genrules with distinct action digests."""

def remote_actions(count):
    for i in range(count):
        native.genrule(
            name = "r%d" % i,
            outs = ["r%d.txt" % i],
            cmd = "echo 'remote action %d' > $@" % i,
            tags = ["no-local"],
        )
