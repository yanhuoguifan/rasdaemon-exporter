package collector

// supportedTableNames is deliberately empty until a table projection and its
// metric contract are implemented together. Discovering a SQLite table alone
// never makes it safe for the exporter to collect that table.
var supportedTableNames = []string{}

// matchingSupportedTables returns registered collectors whose tables exist.
// Keeping discovery and registration separate prevents unknown tables from
// being treated as supported merely because they are present in the database.
func matchingSupportedTables(databaseTables []string) []string {
	var matches []string
	for _, supported := range supportedTableNames {
		for _, discovered := range databaseTables {
			if supported == discovered {
				matches = append(matches, supported)
				break
			}
		}
	}
	return matches
}
