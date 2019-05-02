<?php

/**
 * Class PurgeVersionedDuplicates
 *
 * Purge orphaned DataObjects from the database, and moved from model/Versioned.php
 */
class PurgeVersionedDuplicates extends BuildTask
{
    protected $title = 'Purge Duplicates Version Objects';

    protected $description = 'This task removes duplicate or orphaned versioned objects that may have manifested 
	from a bug in Versioned. It can take a significant amount of time to execute on large datasets.';

    /**
     * Check that the user has appropriate permissions to execute this task
     */
    public function init()
    {
        if (!Director::is_cli() && !Director::isDev() && !Permission::check('ADMIN')) {
            return Security::permissionFailure();
        }

        parent::init();
    }

    /**
     * @param SS_HTTPRequest $HTTPRequest
     * @throws Exception
     */
    public function run($HTTPRequest)
    {
        echo "Starting Purge" . PHP_EOL;

        // Build the database.  Most of the hard work is handled by DataObject
        $dataClasses = ClassInfo::subclassesFor('DataObject');
        array_shift($dataClasses);

        $dbSchema = DB::get_schema();
        $dbSchema->schemaUpdate(function () use ($dataClasses) {
            foreach ($dataClasses as $dataClass) {
                // Instruct the class to apply its schema to the database
                $this->purgeVersionedOrphans($dataClass);
            }
        });

        echo "Finished Successfully" . PHP_EOL;
    }

    /**
     * PurgeVersionedDuplicates
     * @param string $dataClass
     */
    private function purgeVersionedOrphans($dataClass)
    {
        $isRootClass = ($dataClass == ClassInfo::baseDataClass($dataClass));

        // Build a list of suffixes whose tables need versioning
        $allSuffixes = array();
        $versionableExtensions = (array)singleton($dataClass)->config()->versionableExtensions;
        if (count($versionableExtensions)) {
            foreach ($versionableExtensions as $versionableExtension => $suffixes) {
                if (singleton($dataClass)->hasExtension($versionableExtension)) {
                    $allSuffixes = array_merge($allSuffixes, (array)$suffixes);
                    foreach ((array)$suffixes as $suffix) {
                        $allSuffixes[$suffix] = $versionableExtension;
                    }
                }
            }
        }

        // Add the default table with an empty suffix to the list (table name = class name)
        array_push($allSuffixes, '');
        foreach ($allSuffixes as $key => $suffix) {
            // check that this is a valid suffix
            if (!is_int($key)) {
                continue;
            }

            $table = $suffix ? "{$dataClass}_$suffix" : $dataClass;
            if (DB::get_schema()->hasTable("{$table}_versions")) {
                echo "Purging " . $table . PHP_EOL;

                // Fix data that lacks the uniqueness constraint (since this was added later and bugs meant that
                // the constraint was validated)
                $duplications = DB::query(
                    "SELECT MIN(\"ID\") AS \"ID\", \"RecordID\", \"Version\"
                    FROM \"{$table}_versions\" GROUP BY \"RecordID\", \"Version\"
                    HAVING COUNT(*) > 1
                    ORDER BY NULL
                ");

                foreach ($duplications as $dup) {
                    DB::alteration_message("Removing {$table}_versions duplicate data for "
                        . "{$dup['RecordID']}/{$dup['Version']}", "deleted");
                    DB::prepared_query(
                        "DELETE FROM \"{$table}_versions\" WHERE \"RecordID\" = ?
                        AND \"Version\" = ? AND \"ID\" != ?",
                        array($dup['RecordID'], $dup['Version'], $dup['ID'])
                    );
                }

                // Remove junk which has no data in parent classes. Only needs to run the following when versioned
                // data is spread over multiple tables
                if (!$isRootClass && ($versionedTables = ClassInfo::dataClassesFor($table))) {
                    foreach ($versionedTables as $child) {
                        if ($table === $child) {
                            break;
                        } // only need subclasses

                        // Select all orphaned version records
                        $orphanedQuery = SQLSelect::create()
                            ->selectField("\"{$table}_versions\".\"ID\"")
                            ->setFrom("\"{$table}_versions\"");

                        // If we have a parent table limit orphaned records
                        // to only those that exist in this
                        if (DB::get_schema()->hasTable("{$child}_versions")) {
                            $orphanedQuery
                                ->addLeftJoin(
                                    "{$child}_versions",
                                    "\"{$child}_versions\".\"RecordID\" = \"{$table}_versions\".\"RecordID\"
                                    AND \"{$child}_versions\".\"Version\" = \"{$table}_versions\".\"Version\""
                                )
                                ->addWhere("\"{$child}_versions\".\"ID\" IS NULL");
                        }

                        $count = $orphanedQuery->count();
                        if ($count > 0) {
                            DB::alteration_message("Removing {$count} orphaned versioned records", "deleted");
                            $ids = $orphanedQuery->execute()->column();
                            foreach ($ids as $id) {
                                DB::prepared_query(
                                    "DELETE FROM \"{$table}_versions\" WHERE \"ID\" = ?",
                                    array($id)
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
