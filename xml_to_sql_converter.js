const fs = require('fs');
const xml2js = require('xml2js');

// Configuration
const config = {
    inputFile: 'C:\\Program Files\\TallyPrime\\daybook_with_stock_mapping.xml',
    outputFile: 'daybook_export.sql',
    tableName: 'daybook_entries',
    createTable: true,
    useTransaction: true,
    addIndexes: true,
    dateFormat: 'mysql' // mysql, postgres, sqlserver, oracle
};

// Date formatting function
function formatDate(dateStr, format) {
    if (!dateStr) return 'NULL';
    const parts = dateStr.split('-');
    if (parts.length !== 3) return 'NULL';
    const day = parts[0].padStart(2, '0');
    const monthMap = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
        'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
        'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
    };
    const month = monthMap[parts[1]] || '01';
    const year = parts[2].length === 2 ? '20' + parts[2] : parts[2];
    switch(format) {
        case 'mysql':
        case 'postgres':
            return `'${year}-${month}-${day}'`;
        case 'sqlserver':
            return `'${year}${month}${day}'`;
        case 'oracle':
            return `'${day}-${parts[1].toUpperCase()}-${year}'`;
        default:
            return `'${year}-${month}-${day}'`;
    }
}

// SQL string escaping
function escapeSQL(value) {
    if (value === null || value === undefined || value === '') {
        return 'NULL';
    }
    return "'" + String(value).replace(/'/g, "''") + "'";
}

// Clean numeric values
function cleanNumeric(value) {
    if (!value || value === '') return 'NULL';
    let cleaned = value.replace(/,/g, '');
    if (cleaned.includes('(') && cleaned.includes(')')) {
        cleaned = '-' + cleaned.replace(/[()]/g, '');
    }
    const match = cleaned.match(/^-?\d+\.?\d*/);
    return match ? match[0] : 'NULL';
}

// Get field value from row
function getFieldValue(row, fieldName) {
    return row[fieldName] && row[fieldName][0] ? row[fieldName][0].trim() : '';
}

// Generate CREATE TABLE statement
function generateCreateTable(tableName) {
    return `-- Create Table Statement\nCREATE TABLE IF NOT EXISTS ${tableName} (\n    row_number INT PRIMARY KEY,\n    voucher_number INT,\n    date DATE,\n    date_changed INT,\n    vchtype VARCHAR(50),\n    vchno VARCHAR(50),\n    voucher_guid VARCHAR(100),\n    ledger_name VARCHAR(255),\n    voucher_amount DECIMAL(15,2),\n    round_off DECIMAL(15,2),\n    credit_amount DECIMAL(15,2),\n    debit_amount DECIMAL(15,2),\n    ledger_address VARCHAR(255),\n    ledger_city VARCHAR(100),\n    ledger_pincode VARCHAR(20),\n    ledger_state VARCHAR(100),\n    ledger_country VARCHAR(100),\n    parent_group VARCHAR(100),\n    gst_registration VARCHAR(50),\n    opening_balance_ledger DECIMAL(15,2),\n    closing_balance DECIMAL(15,2),\n    narration TEXT,\n    stock_item_number INT,\n    stock_guid VARCHAR(100),\n    stock_item_name VARCHAR(255),\n    stock_date_context DATE,\n    stock_rate VARCHAR(50),\n    stock_actual_qty VARCHAR(50),\n    stock_amount DECIMAL(15,2),\n    stock_base_units VARCHAR(20),\n    stock_billed_qty VARCHAR(50),\n    stock_discount DECIMAL(15,2),\n    has_stock_data VARCHAR(10),\n    opening_qty VARCHAR(50),\n    opening_value DECIMAL(15,2),\n    inwards_qty VARCHAR(50),\n    inwards_value DECIMAL(15,2),\n    inwards_rate VARCHAR(50),\n    outwards_qty VARCHAR(50),\n    outwards_value DECIMAL(15,2),\n    outwards_rate VARCHAR(50),\n    closing_qty VARCHAR(50),\n    closing_value DECIMAL(15,2),\n    net_change_qty VARCHAR(50),\n    net_change_value DECIMAL(15,2),\n    mapping_method VARCHAR(100),\n    mapping_date DATE,\n    gst_type VARCHAR(50),\n    party_gstin VARCHAR(50)\n);\n\n`;
}

// Generate indexes
function generateIndexes(tableName) {
    return `\n-- Create Indexes for Better Query Performance\nCREATE INDEX idx_${tableName}_date ON ${tableName}(date);\nCREATE INDEX idx_${tableName}_vchtype ON ${tableName}(vchtype);\nCREATE INDEX idx_${tableName}_ledger_name ON ${tableName}(ledger_name);\nCREATE INDEX idx_${tableName}_voucher_guid ON ${tableName}(voucher_guid);\nCREATE INDEX idx_${tableName}_stock_item_name ON ${tableName}(stock_item_name);\nCREATE INDEX idx_${tableName}_date_vchtype ON ${tableName}(date, vchtype);\n`;
}

// Main conversion function
async function convertXMLtoSQL() {
    try {
        // Read XML file
        const xmlData = fs.readFileSync(config.inputFile).toString('utf16le');
        // Parse XML
        const parser = new xml2js.Parser();
        const result = await parser.parseStringPromise(xmlData);
        // Extract rows
        const rows = result.DAYBOOK_EXPORT.DATA_ROWS[0].ROW;
        if (!rows || rows.length === 0) {
            console.error('No data rows found in XML');
            return;
        }
        console.log(`Found ${rows.length} rows to convert`);
        // Start building SQL
        let sql = '';
        // Add transaction start
        if (config.useTransaction) {
            sql += '-- Start Transaction\n';
            sql += 'BEGIN TRANSACTION;\n\n';
        }
        // Add CREATE TABLE statement
        if (config.createTable) {
            sql += generateCreateTable(config.tableName);
        }
        // Add INSERT statements
        sql += '-- Insert Data\n';
        rows.forEach((row, index) => {
            const values = [
                getFieldValue(row, 'ROW_NUMBER') || 'NULL',
                getFieldValue(row, 'VOUCHER_NUMBER') || 'NULL',
                formatDate(getFieldValue(row, 'DATE'), config.dateFormat),
                getFieldValue(row, 'DATE_CHANGED') || 'NULL',
                escapeSQL(getFieldValue(row, 'VCHTYPE')),
                escapeSQL(getFieldValue(row, 'VCHNO')),
                escapeSQL(getFieldValue(row, 'VOUCHER_GUID')),
                escapeSQL(getFieldValue(row, 'LEDGER_NAME')),
                cleanNumeric(getFieldValue(row, 'VOUCHER_AMOUNT')),
                cleanNumeric(getFieldValue(row, 'ROUND_OFF')),
                cleanNumeric(getFieldValue(row, 'CREDIT_AMOUNT')),
                cleanNumeric(getFieldValue(row, 'DEBIT_AMOUNT')),
                escapeSQL(getFieldValue(row, 'LEDGER_ADDRESS')),
                escapeSQL(getFieldValue(row, 'LEDGER_CITY')),
                escapeSQL(getFieldValue(row, 'LEDGER_PINCODE')),
                escapeSQL(getFieldValue(row, 'LEDGER_STATE')),
                escapeSQL(getFieldValue(row, 'LEDGER_COUNTRY')),
                escapeSQL(getFieldValue(row, 'PARENT_GROUP')),
                escapeSQL(getFieldValue(row, 'GST_REGISTRATION')),
                cleanNumeric(getFieldValue(row, 'OPENING_BALANCE_LEDGER')),
                cleanNumeric(getFieldValue(row, 'CLOSING_BALANCE')),
                escapeSQL(getFieldValue(row, 'NARRATION')),
                getFieldValue(row, 'STOCK_ITEM_NUMBER') || 'NULL',
                escapeSQL(getFieldValue(row, 'STOCK_GUID')),
                escapeSQL(getFieldValue(row, 'STOCK_ITEM_NAME')),
                formatDate(getFieldValue(row, 'STOCK_DATE_CONTEXT'), config.dateFormat),
                escapeSQL(getFieldValue(row, 'STOCK_RATE')),
                escapeSQL(getFieldValue(row, 'STOCK_ACTUAL_QTY')),
                cleanNumeric(getFieldValue(row, 'STOCK_AMOUNT')),
                escapeSQL(getFieldValue(row, 'STOCK_BASE_UNITS')),
                escapeSQL(getFieldValue(row, 'STOCK_BILLED_QTY')),
                cleanNumeric(getFieldValue(row, 'STOCK_DISCOUNT')),
                escapeSQL(getFieldValue(row, 'HAS_STOCK_DATA')),
                escapeSQL(getFieldValue(row, 'OPENING_QTY')),
                cleanNumeric(getFieldValue(row, 'OPENING_VALUE')),
                escapeSQL(getFieldValue(row, 'INWARDS_QTY')),
                cleanNumeric(getFieldValue(row, 'INWARDS_VALUE')),
                escapeSQL(getFieldValue(row, 'INWARDS_RATE')),
                escapeSQL(getFieldValue(row, 'OUTWARDS_QTY')),
                cleanNumeric(getFieldValue(row, 'OUTWARDS_VALUE')),
                escapeSQL(getFieldValue(row, 'OUTWARDS_RATE')),
                escapeSQL(getFieldValue(row, 'CLOSING_QTY')),
                cleanNumeric(getFieldValue(row, 'CLOSING_VALUE')),
                escapeSQL(getFieldValue(row, 'NET_CHANGE_QTY')),
                cleanNumeric(getFieldValue(row, 'NET_CHANGE_VALUE')),
                escapeSQL(getFieldValue(row, 'MAPPING_METHOD')),
                formatDate(getFieldValue(row, 'MAPPING_DATE'), config.dateFormat),
                escapeSQL(getFieldValue(row, 'GST_TYPE')),
                escapeSQL(getFieldValue(row, 'PARTY_GSTIN'))
            ];
            sql += `INSERT INTO ${config.tableName} VALUES (\n`;
            sql += values.map(v => `    ${v}`).join(',\n');
            sql += '\n);\n';
            if (index < rows.length - 1) {
                sql += '\n';
            }
        });
        // Add indexes
        if (config.addIndexes) {
            sql += generateIndexes(config.tableName);
        }
        // Add transaction commit
        if (config.useTransaction) {
            sql += '\n-- Commit Transaction\n';
            sql += 'COMMIT;\n';
        }
        // Write SQL file
        fs.writeFileSync(config.outputFile, sql);
        console.log(`✅ SQL file generated successfully: ${config.outputFile}`);
        console.log(`📊 Total rows converted: ${rows.length}`);
        console.log(`💾 Output file size: ${(sql.length / 1024).toFixed(2)} KB`);
    } catch (error) {
        console.error('❌ Error converting XML to SQL:', error.message);
    }
}

// Run the conversion
convertXMLtoSQL();

// To use this script:
// 1. Install xml2js: npm install xml2js
// 2. Update the config object with your file paths and preferences
// 3. Run: node xml_to_sql_converter.js 