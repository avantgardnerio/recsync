require('dotenv').config()
const fs = require('fs');
const mysql = require(`mysql-await`);
const yargs = require('yargs');

const queryFks = fs.readFileSync("db/queries/get_fks.sql", "utf-8");
const queryCols = fs.readFileSync("db/queries/get_columns.sql", "utf-8");

const argv = yargs
    .option('dst-host', {description: 'Destination host', type: 'string'})
    .option('dst-port', {description: 'Destination port', type: 'string'})
    .option('dst-db', {description: 'Destination db name', type: 'string'})
    .option('dst-user', {description: 'Destination user', type: 'string'})
    .option('dst-pass', {description: 'Destination password', type: 'string'})
    .option('seed-table', {description: 'Name of root table to sync', type: 'string'})
    .option('pk', {description: 'Value of primary key to start sync', type: 'array'})
    .help().alias('help', 'h')
    .argv;

const getRels = async (con, db) => {
    const fks = await con.awaitQuery(queryFks, db);
    const rels = fks.reduce((acc, cur) => {
        let obj = acc[cur.CONSTRAINT_NAME] || {name: cur.CONSTRAINT_NAME, cols: []};
        obj.parent = cur.REFERENCED_TABLE_NAME;
        obj.child = cur.TABLE_NAME;
        obj.cols[cur.ORDINAL_POSITION - 1] = {
            parent: cur.REFERENCED_COLUMN_NAME,
            child: cur.COLUMN_NAME,
        };
        acc[cur.CONSTRAINT_NAME] = obj;
        return acc;
    }, {});
    return rels;
};

const indexRels = (rels, fieldName) => {
    const idx = Object.keys(rels).reduce((acc, cur) => {
        const rel = rels[cur];
        const key = rel[fieldName];
        acc[key] = acc[key] || [];
        acc[key].push(rel);
        return acc;
    }, {});
    return idx;
};

const getTables = async (con, db) => {
    const cols = await con.awaitQuery(queryCols, db);
    const tables = cols.reduce((acc, cur) => {
        acc[cur.TABLE_NAME] = acc[cur.TABLE_NAME] || {"name": cur.TABLE_NAME, "cols": {}};
        acc[cur.TABLE_NAME].cols[cur.COLUMN_NAME] = cur;
        return acc;
    }, {});
    return tables;
};

const selectRows = async (table, queryVals, con) => {
    if (queryVals.length === 0) return [];
    queryVals = queryVals.slice(0, 500); // MySQL freaks out with large queries

    // run query
    const sql = `${table.selectSql} (${queryVals.map(() => table.pkExpr).join(' or ')})`;
    const flat = queryVals.reduce((acc, cur) => [...acc, ...cur], []);
    const srcRows = (await srcCon.awaitQuery(sql, flat));
    const insertVals = srcRows.map(row => Object.values(row));

    // convert MariaDB nulls to MySQL nulls
    const keys = Object.keys(table.cols);
    for(let idx = 0; idx < keys.length; idx++) {
        const key = keys[idx];
        const col = table.cols[key];
        if(col.COLUMN_TYPE.startsWith('enum')) {
            insertVals.filter(row => row[idx] === '').forEach(row => {
                row[idx] = null
            });
        }
    }

    return insertVals;
};

const insertRows = async (con, table, values) => {
    if(values.length === 0) return;
    const sql = `${table.insertSql} ${values.map(row => `(${row.map(() => `?`).join(', ')})`).join(', ')}`;
    const flat = values.reduce((acc, cur) => [...acc, ...cur], []);
        const res = await con.awaitQuery(sql, flat);
        if (res.affectedRows !== values.length) {
            console.warn(`Problem inserting into ${table.tableName} ${table.pkExpr} affected ${res.affectedRows} rows ${res.message}`);
    }
}

const deleteRows = async (con, table, values) => {
    if(values.length === 0) return;
    const sql = `${table.deleteSql} (${values.map(() => table.pkExpr).join(' or ')})`;
    const flat = values.reduce((acc, cur) => [...acc, ...cur], []);
    const res = await con.awaitQuery(sql, flat);
};

const saveRows = async(db, con) => {
    for(let tableName of Object.keys(db.tables)) {
        const table = db.tables[tableName];
        if(Object.keys(table.rows).length === 0) continue;
        console.log(`Saving ${tableName}`);
        const insertVals = Object.values(table.rows).map(row => row.values);
        while(insertVals.length > 0) {
            const insertPage = insertVals.splice(0, 500);
            const deleteVals = insertPage.map(row => table.pkIdxs.map(idx => row[idx]));
            await deleteRows(con, table, deleteVals);
            await insertRows(con, table, insertPage)
        }
    }
};

const loadRows = async (db, tableName, queryVals, con) => {
    let loaded = false;
    if (queryVals.length === 0) return [];
    const table = db.tables[tableName];
    while (queryVals.length > 0) {
        console.log(`${queryVals.length} ${tableName} entries remaining...`);
        const page = queryVals.splice(0, 500); // MySQL freaks out with large queries
        const rows = await selectRows(table, page, con);
        for (let row of rows) {
            const pk = table.pkIdxs.map(idx => row[idx]);
            const pkStr = JSON.stringify(pk);
            if (table.rows[pkStr] === undefined) {
                table.rows[pkStr] = {values: row, loaded: false};
                loaded = true;
            }
        }
    }
    return loaded;
}

const loadParents = async (db, relationship, relatedRows, con) => {
    const thisTableName = relationship.parent;
    const thisTable = db.tables[thisTableName];
    if(thisTable === undefined) {
        console.log(`Skipping new table ${thisTableName}...`);
        return; // Doesn't exist in source
    }
    const otherTable = db.tables[relationship.child];
    const fkCols = relationship.cols.map(col => col.parent);
    const fkExpr = `(${fkCols.map(col => `\`${col}\`=?`).join(' and ')})`;
    const selectSql = `select ${thisTable.pkCols.join(', ')}
                       from \`${thisTableName}\`
                       where `;

    const indices = relationship.cols.map(col => otherTable.colNames.indexOf(`\`${col.child}\``));
    let otherVals = relatedRows.map(row => indices.map(idx => row.values[idx]));
    let obj = otherVals.reduce((acc, cur) => {
        const key = JSON.stringify(cur);
        acc[key] = cur;
        return acc;
    }, {});
    otherVals = Object.values(obj); // dedup all the FKs pointing at the same parent

    const thisVals = [];
    while (otherVals.length > 0) {
        const pageVals = otherVals.splice(0, 500); // MySQL freaks out with large queries
        const sql = `${selectSql} (${pageVals.map(() => fkExpr).join(' or ')})`;
        const flat = pageVals.reduce((acc, cur) => {
            acc.push(...cur);
            return acc;
        }, []);
        const thisPkRows = (await srcCon.awaitQuery(sql, flat));
        const tempVals = thisPkRows.map(row => Object.values(row));
        console.log(`child.name=${relationship.name} childVals=${thisVals.length} tempVals=${tempVals.length}`);
        thisVals.push(...tempVals);
        console.log(`child.name=${relationship.name} childVals=${thisVals.length}`);
    }
    return await loadRows(db, thisTableName, thisVals, con);
}

const loadChildren = async (db, relationship, relatedRows, con) => {
    const thisTableName = relationship.child;
    const thisTable = db.tables[thisTableName];
    if(thisTable === undefined) {
        console.log(`Skipping new table ${thisTableName}...`);
        return; // Doesn't exist in source
    }
    const otherTable = db.tables[relationship.parent];
    const fkCols = relationship.cols.map(col => col.child);
    const fkExpr = `(${fkCols.map(col => `\`${col}\`=?`).join(' and ')})`;
    let selectSql = `select ${thisTable.pkCols.join(', ')}
                       from \`${thisTableName}\`
                       where `;
    // if(thisTable.dateClause !== '') {
    //     selectSql += ` (${thisTable.dateClause}) and `;
    // }

    // Get parent FK values
    const indices = relationship.cols.map(col => {
        let name = col.parent;
        return otherTable.colNames.indexOf(`\`${name}\``);
    });
    let otherVals = relatedRows.map(row => indices.map(idx => row.values[idx]));

    const thisVals = [];
    while (otherVals.length > 0) {
        const pageVals = otherVals.splice(0, 500); // MySQL freaks out with large queries
        const sql = `${selectSql} (${pageVals.map(() => fkExpr).join(' or ')}) ${thisTable.dateClause} limit 500`;  // TODO: max row setting
        const flat = pageVals.reduce((acc, cur) => {
            acc.push(...cur);
            return acc;
        }, []);
        // console.log(`Running sql: ${sql}...`);
        const childPkRows = (await srcCon.awaitQuery(sql, flat));
        const tempVals = childPkRows.map(row => Object.values(row));
        console.log(`child.name=${relationship.name} childVals=${thisVals.length} tempVals=${tempVals.length}`);
        tempVals.forEach(v => thisVals.push(v));
        console.log(`child.name=${relationship.name} childVals=${thisVals.length}`);
    }
    return await loadRows(db, thisTableName, thisVals, con);
}

const loadRelated = async (db, con) => {
    let newRows = false;
    for (let tableName of Object.keys(db.tables)) {
        const table = db.tables[tableName];
        const loadable = Object.values(table.rows).filter(row => row.loaded === false);
        if (loadable.length === 0) continue;
        for (let rel of db.parent2child[tableName] || []) {
            console.log(`Loading children...`);
            newRows |= await loadChildren(db, rel, loadable, con);
        }
        for (let rel of db.child2parent[tableName] || []) {
            console.log(`Loading parents...`);
            newRows |= await loadParents(db, rel, loadable, con);
        }
        loadable.forEach(row => row.loaded = true);
    }
    return newRows;
}

const srcConfig = {
    "connectionLimit": 10,
    "host": process.env.DATABASE_PARAMS_HOST,
    "port": process.env.DATABASE_PARAMS_PORT,
    "database": process.env.DATABASE_PARAMS_DBNAME,
    "user": process.env.DATABASE_PARAMS_USERNAME,
    "password": process.env.DATABASE_PARAMS_PASSWORD,
};
const dstConfig = {
    "connectionLimit": 10,
    "host": argv['dst-host'] || '127.0.0.1',
    "port": argv['dst-port'] || '3306',
    "database": argv['dst-db'],
    "user": argv['dst-user'] || 'root',
    "password": argv['dst-pass'] || '',
};
const srcCon = mysql.createConnection(srcConfig);
srcCon.on(`error`, (err) => console.error(`Connection error ${err.code}`));
const dstCon = mysql.createConnection(dstConfig);
dstCon.on(`error`, (err) => console.error(`Connection error ${err.code}`));
console.log(`Syncing ${srcConfig.host}:${srcConfig.port} -> ${dstConfig.host}:${dstConfig.port}`);

const db = {};

(async () => {
    await dstCon.awaitQuery(`SET FOREIGN_KEY_CHECKS=0;`);
    await dstCon.awaitQuery(`SET UNIQUE_CHECKS=0;`);
    try {
        const start = new Date().getTime();
        const physical_rels = await getRels(dstCon, dstConfig.database);
        fs.writeFileSync('physical_rels.json', JSON.stringify(physical_rels, undefined, 3), {encoding: "utf8"});
        // const physical_rels = JSON.parse(fs.readFileSync('physical_rels.json', 'utf-8'));
        const blacklist = JSON.parse(fs.readFileSync('blacklist.json', 'utf-8'));
        blacklist.forEach(relName => delete physical_rels[relName]);
        const logical_rels = JSON.parse(fs.readFileSync('logical_rels.json', 'utf-8'));
        db.tables = await getTables(srcCon, srcConfig.database);
        db.rels = {...physical_rels, ...logical_rels};
        db.parent2child = indexRels(db.rels, 'parent');
        db.child2parent = indexRels(db.rels, 'child');
        for (let tableName of Object.keys(db.tables)) {
            const table = db.tables[tableName];
            const dateTypes = ['timestamp', 'date', 'datetime'];
            table.colNames = Object.values(table.cols).map(col => `\`${col.COLUMN_NAME}\``);
            table.pkCols = Object.values(table.cols).filter(col => col.COLUMN_KEY === 'PRI').map(col => col.COLUMN_NAME);
            table.pkIdxs = Object.values(table.cols).reduce((acc, cur, idx) => [...acc, ...(cur.COLUMN_KEY === 'PRI' ? [idx] : [])], []);
            table.dateFields = Object.values(table.cols).filter(col => dateTypes.includes(col.DATA_TYPE)).map(col => col.COLUMN_NAME);
            if(table.dateFields.length === 0) {
                table.dateClause = '';
            } else if(table.dateFields.length === 1) {
                table.dateClause = `order by ${table.dateFields[0]} desc`;
            } else {
                table.dateClause = `order by GREATEST(${table.dateFields.map(c => `\`${c}\``).join(', ')}) desc`;
            }
            table.selectSql = `select ${table.colNames.join(', ')} from \`${tableName}\` where `;
            table.insertSql = `insert into \`${tableName}\` (${table.colNames.join(', ')}) values `;
            table.deleteSql = `delete from \`${tableName}\` where `;
            table.pkExpr = `(${table.pkCols.map(col => `\`${col}\`=?`).join(' and ')})`;
            table.rows = {};
        }

        const tableName = argv['seed-table'];
        const pkVals = argv['pk'];
        const queryVals = [];
        queryVals.push(...pkVals.map(it => [it]));

        await loadRows(db, tableName, queryVals, srcCon);

        while (await loadRelated(db, srcCon)) {
            console.log('Loading more rows...');
        }

        await saveRows(db, dstCon);

        const end = new Date().getTime();
        console.log(`Sync complete in ${Math.round((end - start) / 1000)} sec`);
    } finally {
        try {
            console.log(`Enabling FK checks...`);
            await dstCon.awaitQuery(`SET FOREIGN_KEY_CHECKS=1;`);
            console.log(`Enabling unique checks...`);
            await dstCon.awaitQuery(`SET UNIQUE_CHECKS=1;`);
        } catch (ex) {
            console.warn("Unable to re-enable constraints. Try running it again.")
        }
    }

    console.log(`Closing connections...`);
    await srcCon.awaitEnd();
    await dstCon.awaitEnd();
    console.log(`Done!`);
})().catch((ex) => {
    console.error("Error synchronizing databases!", ex);
    srcCon
        .awaitEnd()
        .then(() => dstCon.awaitEnd())
        .finally(() => {
            process.exit(1);
        });
});
