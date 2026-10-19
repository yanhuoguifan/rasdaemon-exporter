package collector

// supportedTableNames lists only tables with a complete projection and metric
// contract. Unknown database tables remain outside the collection path.
var supportedTableNames = []string{"devlink_event"}

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
