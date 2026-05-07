package customwrappers_test

import (
	"context"
	"fmt"
	"slices"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/examples/scripted"
)

type PackageAudit struct {
	Name      string
	Version   string
	Installed bool
}

func AuditPackage(
	ctx context.Context,
	client *brine.Client,
	target brine.Target,
	name string,
) (map[string]PackageAudit, error) {
	result, err := client.Run(ctx, brine.Local(
		"pkg.version",
		target,
		brine.Args(name),
		brine.Metadata("wrapper", "AuditPackage"),
	))
	if err != nil {
		return nil, err
	}

	versions, err := brine.DecodeByMinion[string](result)
	if err != nil {
		return nil, err
	}

	audits := make(map[string]PackageAudit, len(versions))
	for minion, version := range versions {
		audits[minion] = PackageAudit{
			Name:      name,
			Version:   version,
			Installed: version != "",
		}
	}

	return audits, nil
}

func Example_customTypedWrapper() {
	transport := scripted.New(map[string]scripted.Scenario{
		scripted.LocalRun("pkg.version"): {
			JID:      "jid-pkg-version",
			Expected: []string{"web-1", "web-2"},
			Returns: []scripted.Return{
				{Minion: "web-1", Value: "1.2.3"},
				{Minion: "web-2", Value: ""},
			},
		},
	})

	client, err := brine.New(transport)
	if err != nil {
		panic(err)
	}

	audits, err := AuditPackage(context.Background(), client, brine.List("web-1", "web-2"), "nginx")
	if err != nil {
		panic(err)
	}

	minions := make([]string, 0, len(audits))
	for minion := range audits {
		minions = append(minions, minion)
	}
	slices.Sort(minions)

	for _, minion := range minions {
		audit := audits[minion]
		version := audit.Version
		if version == "" {
			version = "(not installed)"
		}

		fmt.Printf("%s %s installed=%t version=%s\n", minion, audit.Name, audit.Installed, version)
	}

	// Output:
	// web-1 nginx installed=true version=1.2.3
	// web-2 nginx installed=false version=(not installed)
}
