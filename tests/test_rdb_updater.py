"""Testing for RDBUpdater.
"""
from rdb_updater import RDBUpdater


class TestRDBUpdater:
    """Testing for RDB with MySQL database"""

    def init(self, rdb_updater_mysql_gff):
        """Testing for RDB.init()"""
        assert isinstance(rdb_updater_mysql_gff, RDBUpdater)

    def update_all_database_tables(self, rdb_updater_mysql_gff):
        """Testing for update_all_database_tables()"""
        assert rdb_updater_mysql_gff.rdb.get_table_names() == []
        rdb_updater_mysql_gff.update_all_database_tables()
        assert rdb_updater_mysql_gff.rdb.get_table_names() == [
            "Donor",
            "AnimalModel",
            "CellLine",
            "Antibody",
            "GeneticReagent",
            "Funder",
            "Publication",
            "Investigator",
            "Resource",
            "MutationDetails",
            "Vendor",
            "Development",
            "Mutation",
            "ResourceApplication",
            "Observation",
            "VendorItem",
            "Biobank",
            "Usage",
        ]
