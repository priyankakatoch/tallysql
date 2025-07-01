const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');

// Load environment variables
require('dotenv').config();

console.log('🚀 Starting Tally Daybook XML to MySQL Converter...');

// Configuration from environment variables
const config = {
    db: {
        host: process.env.DB_HOST || 'localhost',
        user: process.env.DB_USER || 'root',
        password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME || 'ps',
        port: process.env.DB_PORT || 3306
    },
    daybookXmlPath: process.env.DAYBOOK_XML_PATH || 'C:\\Program Files\\TallyPrime\\daybook_with_stock_mapping.xml',
    debug: process.env.DEBUG === 'true'
};

// Function to read XML file with proper encoding handling
function readXMLFile(filePath) {
    try {
        console.log(`🔍 Reading file from: ${filePath}`);
        
        const buffer = fs.readFileSync(filePath);
        console.log(`📏 File size: ${buffer.length} bytes`);
        
        let content;
        
        // Check for UTF-16 LE BOM (FF FE)
        if (buffer.length >= 2 && buffer[0] === 0xFF && buffer[1] === 0xFE) {
            console.log('🔍 UTF-16 LE BOM detected and removing...');
            const utf16Buffer = buffer.slice(2);
            content = utf16Buffer.toString('utf16le');
        }
        // Check for UTF-16 BE BOM (FE FF)
        else if (buffer.length >= 2 && buffer[0] === 0xFE && buffer[1] === 0xFF) {
            console.log('🔍 UTF-16 BE BOM detected, converting...');
            const swapped = Buffer.alloc(buffer.length - 2);
            for (let i = 2; i < buffer.length; i += 2) {
                swapped[i - 2] = buffer[i + 1];
                swapped[i - 1] = buffer[i];
            }
            content = swapped.toString('utf16le');
        }
        // Check for UTF-8 BOM (EF BB BF)
        else if (buffer.length >= 3 && buffer[0] === 0xEF && buffer[1] === 0xBB && buffer[2] === 0xBF) {
            console.log('🔍 UTF-8 BOM detected, removing...');
            content = buffer.slice(3).toString('utf8');
        }
        // Try UTF-8 without BOM
        else {
            console.log('🔍 No BOM detected, trying UTF-8...');
            content = buffer.toString('utf8');
        }
        
        console.log(`📄 Content length: ${content.length} characters`);
        
        // Clean content - remove control characters but preserve line breaks
        content = content.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '');
        
        return content;
    } catch (error) {
        console.error('❌ Error reading XML file:', error.message);
        throw error;
    }
}

// Enhanced function to parse ALL Tally daybook records from XML
function parseAllDaybookRecords(xmlContent) {
    const records = [];
    
    try {
        console.log('🔍 Starting comprehensive daybook XML parsing...');
        
        // First, let's analyze the XML structure
        console.log('🔍 Analyzing XML structure...');
        const firstPart = xmlContent.substring(0, 5000);
        console.log('📄 First 5000 characters of XML:');
        console.log(firstPart);
        
        // Look for common Tally XML patterns
        const possiblePatterns = [
            /<ENVELOPE>/gi,
            /<VOUCHER>/gi,
            /<TALLYMESSAGE>/gi,
            /<DAYBOOK>/gi,
            /<ENTRY>/gi,
            /<TRANSACTION>/gi,
            /<VCHTYPE>/gi,
            /<VCHNUMBER>/gi,
            /<DATE>/gi,
            /<AMOUNT>/gi
        ];
        
        console.log('🔍 Looking for XML patterns:');
        possiblePatterns.forEach(pattern => {
            const matches = xmlContent.match(pattern);
            if (matches) {
                console.log(`Found ${matches.length} instances of ${pattern.source}`);
            }
        });
        
        // Method 1: Try different envelope patterns
        let envelopeMatches = xmlContent.match(/<ENVELOPE>[\s\S]*?<\/ENVELOPE>/gi);
        if (!envelopeMatches) {
            envelopeMatches = xmlContent.match(/<TALLYMESSAGE>[\s\S]*?<\/TALLYMESSAGE>/gi);
        }
        if (!envelopeMatches) {
            envelopeMatches = xmlContent.match(/<VOUCHER>[\s\S]*?<\/VOUCHER>/gi);
        }
        if (!envelopeMatches) {
            envelopeMatches = xmlContent.match(/<DAYBOOK>[\s\S]*?<\/DAYBOOK>/gi);
        }
        
        if (envelopeMatches && envelopeMatches.length > 0) {
            console.log(`📦 Found ${envelopeMatches.length} data blocks`);
            
            envelopeMatches.forEach((envelope, index) => {
                const record = parseEnvelopeBlock(envelope);
                if (record && hasValidDaybookData(record)) {
                    records.push(record);
                }
                
                // Show first few blocks for debugging
                if (index < 3) {
                    console.log(`🔍 Block ${index + 1} content (first 500 chars):`);
                    console.log(envelope.substring(0, 500));
                }
                
                // Progress indicator for large files
                if ((index + 1) % 1000 === 0) {
                    console.log(`📈 Processed ${index + 1}/${envelopeMatches.length} data blocks`);
                }
            });
        } else {
            console.log('⚠️  No standard blocks found, trying line-by-line parsing...');
            
            // Method 2: Parse line by line looking for any XML tags
            const lines = xmlContent.split('\n');
            let currentRecord = {};
            let recordsFound = 0;
            
            for (let i = 0; i < lines.length; i++) {
                const line = lines[i].trim();
                
                // Skip empty lines and comments
                if (!line || line.startsWith('<!--') || line.startsWith('<?xml')) {
                    continue;
                }
                
                // Look for any XML tag with content
                const tagMatches = line.match(/<([^>\/\s]+)>(.*?)<\/\1>/g);
                if (tagMatches) {
                    tagMatches.forEach(tagMatch => {
                        const fieldMatch = tagMatch.match(/<([^>\/\s]+)>(.*?)<\/\1>/);
                        if (fieldMatch) {
                            const fieldName = fieldMatch[1].toUpperCase();
                            const fieldValue = fieldMatch[2].trim();
                            
                            if (fieldValue) {
                                currentRecord[fieldName] = fieldValue;
                                
                                // Show first few discovered fields
                                if (recordsFound < 10) {
                                    console.log(`🔍 Found field: ${fieldName} = ${fieldValue}`);
                                }
                            }
                        }
                    });
                }
                
                // If we found some data, check if we should save this record
                if (Object.keys(currentRecord).length > 0) {
                    // Look for end-of-record indicators or when we have enough fields
                    if (Object.keys(currentRecord).length >= 3 || 
                        line.includes('</') || 
                        (i > 0 && lines[i + 1] && lines[i + 1].trim().startsWith('<') && 
                         !lines[i + 1].includes(Object.keys(currentRecord)[0]))) {
                        
                        if (hasValidDaybookData(currentRecord)) {
                            records.push({ ...currentRecord });
                            recordsFound++;
                            
                            if (recordsFound <= 3) {
                                console.log(`🔍 Record ${recordsFound}:`, JSON.stringify(currentRecord, null, 2));
                            }
                        }
                        currentRecord = {};
                    }
                }
                
                // Progress indicator
                if ((i + 1) % 10000 === 0) {
                    console.log(`📈 Processed ${i + 1}/${lines.length} lines, found ${recordsFound} records so far`);
                }
            }
            
            // Don't forget the last record
            if (hasValidDaybookData(currentRecord)) {
                records.push(currentRecord);
                recordsFound++;
            }
        }
        
        console.log(`✅ Parsed ${records.length} valid daybook records from XML`);
        
        // Show sample records for debugging
        if (records.length > 0) {
            console.log('🔍 First few daybook records:');
            for (let i = 0; i < Math.min(3, records.length); i++) {
                console.log(`Daybook Record ${i + 1}:`, JSON.stringify(records[i], null, 2));
            }
        } else {
            console.log('⚠️  No valid records found. Let me show you some sample XML content to help debug:');
            const sampleLines = xmlContent.split('\n').slice(0, 50);
            sampleLines.forEach((line, index) => {
                if (line.trim()) {
                    console.log(`Line ${index + 1}: ${line.trim()}`);
                }
            });
        }
        
        return records;
    } catch (error) {
        console.error('❌ Error parsing daybook XML records:', error.message);
        return [];
    }
}

// Function to parse individual data block for daybook
function parseEnvelopeBlock(blockXML) {
    const record = {};
    
    try {
        // Define all possible fields to extract for daybook - expanded list
        const fieldPatterns = {
            // Date fields - all possible variations
            'DATE': /<DATE>(.*?)<\/DATE>/i,
            'VCHDATE': /<VCHDATE>(.*?)<\/VCHDATE>/i,
            'DSPVCHDATE': /<DSPVCHDATE>(.*?)<\/DSPVCHDATE>/i,
            'EFFECTIVEDATE': /<EFFECTIVEDATE>(.*?)<\/EFFECTIVEDATE>/i,
            'TRANSACTIONDATE': /<TRANSACTIONDATE>(.*?)<\/TRANSACTIONDATE>/i,
            'ENTRYDATE': /<ENTRYDATE>(.*?)<\/ENTRYDATE>/i,
            
            // Voucher type fields
            'VCHTYPE': /<VCHTYPE>(.*?)<\/VCHTYPE>/i,
            'DSPVCHTYPE': /<DSPVCHTYPE>(.*?)<\/DSPVCHTYPE>/i,
            'VOUCHERTYPENAME': /<VOUCHERTYPENAME>(.*?)<\/VOUCHERTYPENAME>/i,
            'TYPE': /<TYPE>(.*?)<\/TYPE>/i,
            'VOUCHER_TYPE': /<VOUCHER_TYPE>(.*?)<\/VOUCHER_TYPE>/i,
            
            // Voucher number fields
            'VCHNUMBER': /<VCHNUMBER>(.*?)<\/VCHNUMBER>/i,
            'VOUCHERKEY': /<VOUCHERKEY>(.*?)<\/VOUCHERKEY>/i,
            'DSPEXPLVCHNUMBER': /<DSPEXPLVCHNUMBER>(.*?)<\/DSPEXPLVCHNUMBER>/i,
            'EXPLVCHNUMBER': /<EXPLVCHNUMBER>(.*?)<\/EXPLVCHNUMBER>/i,
            'NUMBER': /<NUMBER>(.*?)<\/NUMBER>/i,
            'VOUCHER_NUMBER': /<VOUCHER_NUMBER>(.*?)<\/VOUCHER_NUMBER>/i,
            'VCH_NO': /<VCH_NO>(.*?)<\/VCH_NO>/i,
            'SERIALNUMBER': /<SERIALNUMBER>(.*?)<\/SERIALNUMBER>/i,
            
            // Amount fields - Debit (all variations)
            'DRAMT': /<DRAMT>(.*?)<\/DRAMT>/i,
            'DSPVCHDRAMT': /<DSPVCHDRAMT>(.*?)<\/DSPVCHDRAMT>/i,
            'LVSUBDRTOTAL': /<LVSUBDRTOTAL>(.*?)<\/LVSUBDRTOTAL>/i,
            'DEBITAMOUNT': /<DEBITAMOUNT>(.*?)<\/DEBITAMOUNT>/i,
            'DEBIT': /<DEBIT>(.*?)<\/DEBIT>/i,
            'DR_AMOUNT': /<DR_AMOUNT>(.*?)<\/DR_AMOUNT>/i,
            'AMOUNT_DR': /<AMOUNT_DR>(.*?)<\/AMOUNT_DR>/i,
            
            // Amount fields - Credit (all variations)
            'CRAMT': /<CRAMT>(.*?)<\/CRAMT>/i,
            'DSPVCHCRAMT': /<DSPVCHCRAMT>(.*?)<\/DSPVCHCRAMT>/i,
            'LVSUBCRTOTAL': /<LVSUBCRTOTAL>(.*?)<\/LVSUBCRTOTAL>/i,
            'CREDITAMOUNT': /<CREDITAMOUNT>(.*?)<\/CREDITAMOUNT>/i,
            'CREDIT': /<CREDIT>(.*?)<\/CREDIT>/i,
            'CR_AMOUNT': /<CR_AMOUNT>(.*?)<\/CR_AMOUNT>/i,
            'AMOUNT_CR': /<AMOUNT_CR>(.*?)<\/AMOUNT_CR>/i,
            
            // General amount fields
            'AMOUNT': /<AMOUNT>(.*?)<\/AMOUNT>/i,
            'VALUE': /<VALUE>(.*?)<\/VALUE>/i,
            'TOTAL': /<TOTAL>(.*?)<\/TOTAL>/i,
            
            // Additional fields that might be useful
            'LEDGERNAME': /<LEDGERNAME>(.*?)<\/LEDGERNAME>/i,
            'DSPVCHLEDACCOUNT': /<DSPVCHLEDACCOUNT>(.*?)<\/DSPVCHLEDACCOUNT>/i,
            'PARTYLEDGERNAME': /<PARTYLEDGERNAME>(.*?)<\/PARTYLEDGERNAME>/i,
            'REFERENCE': /<REFERENCE>(.*?)<\/REFERENCE>/i,
            'NARRATION': /<NARRATION>(.*?)<\/NARRATION>/i,
            'DESCRIPTION': /<DESCRIPTION>(.*?)<\/DESCRIPTION>/i,
            'PARTICULARS': /<PARTICULARS>(.*?)<\/PARTICULARS>/i
        };
        
        // Extract each field
        for (const [fieldName, pattern] of Object.entries(fieldPatterns)) {
            const match = blockXML.match(pattern);
            if (match && match[1] && match[1].trim()) {
                record[fieldName] = match[1].trim();
            }
        }
        
        // Also try to extract any XML tag that looks like it might contain data
        const allTagMatches = blockXML.match(/<([A-Z_][A-Z0-9_]*?)>(.*?)<\/\1>/gi);
        if (allTagMatches) {
            allTagMatches.forEach(tagMatch => {
                const fieldMatch = tagMatch.match(/<([A-Z_][A-Z0-9_]*?)>(.*?)<\/\1>/i);
                if (fieldMatch && fieldMatch[2] && fieldMatch[2].trim()) {
                    const fieldName = fieldMatch[1].toUpperCase();
                    const fieldValue = fieldMatch[2].trim();
                    
                    // Don't overwrite existing fields, but add new ones
                    if (!record[fieldName]) {
                        record[fieldName] = fieldValue;
                    }
                }
            });
        }
        
        return record;
    } catch (error) {
        if (config.debug) {
            console.error('Error parsing data block:', error.message);
        }
        return null;
    }
}

// Function to check if daybook record has valid data
function hasValidDaybookData(record) {
    if (!record || Object.keys(record).length === 0) {
        return false;
    }
    
    // Check for any date field
    const hasDate = record.DATE || record.VCHDATE || record.DSPVCHDATE || 
                   record.EFFECTIVEDATE || record.TRANSACTIONDATE || record.ENTRYDATE;
    
    // Check for any voucher type field
    const hasVoucherType = record.VCHTYPE || record.DSPVCHTYPE || record.VOUCHERTYPENAME || 
                          record.TYPE || record.VOUCHER_TYPE;
    
    // Check for any voucher number field
    const hasVoucherNumber = record.VCHNUMBER || record.DSPEXPLVCHNUMBER || record.EXPLVCHNUMBER || 
                            record.NUMBER || record.VOUCHER_NUMBER || record.VCH_NO || 
                            record.VOUCHERKEY || record.SERIALNUMBER;
    
    // Check for any amount field
    const hasAmount = record.DRAMT || record.DSPVCHDRAMT || record.LVSUBDRTOTAL || 
                     record.DEBITAMOUNT || record.DEBIT || record.DR_AMOUNT || record.AMOUNT_DR ||
                     record.CRAMT || record.DSPVCHCRAMT || record.LVSUBCRTOTAL || 
                     record.CREDITAMOUNT || record.CREDIT || record.CR_AMOUNT || record.AMOUNT_CR ||
                     record.AMOUNT || record.VALUE || record.TOTAL;
    
    // Check for any descriptive field
    const hasDescription = record.LEDGERNAME || record.DSPVCHLEDACCOUNT || record.PARTYLEDGERNAME ||
                          record.REFERENCE || record.NARRATION || record.DESCRIPTION || record.PARTICULARS;
    
    // Consider it valid if it has at least 2 of these categories
    const validFields = [hasDate, hasVoucherType, hasVoucherNumber, hasAmount, hasDescription]
                       .filter(Boolean).length;
    
    // Also log what fields we found for debugging
    if (config.debug && validFields > 0) {
        console.log(`🔍 Record validation - Found ${validFields} categories:`, {
            hasDate, hasVoucherType, hasVoucherNumber, hasAmount, hasDescription,
            sampleFields: Object.keys(record).slice(0, 5)
        });
    }
    
    return validFields >= 1; // Lower threshold to catch more records during debugging
}

// Function to convert parsed daybook records to database format
function convertToDaybookEntries(records) {
    const daybookEntries = [];
    
    console.log(`🔄 Converting ${records.length} records to daybook format...`);
    
    for (let i = 0; i < records.length; i++) {
        try {
            const record = records[i];
            
            const daybookEntry = {
                date: formatDate(
                    record.DATE || 
                    record.VCHDATE || 
                    record.DSPVCHDATE || 
                    record.EFFECTIVEDATE
                ),
                vch_type: (
                    record.VCHTYPE || 
                    record.DSPVCHTYPE || 
                    record.VOUCHERTYPENAME || 
                    ''
                ).substring(0, 50),
                vch_no: (
                    record.VCHNUMBER || 
                    record.DSPEXPLVCHNUMBER || 
                    record.EXPLVCHNUMBER || 
                    record.VOUCHERKEY || 
                    ''
                ).substring(0, 50),
                debit_amount: parseAmount(
                    record.DRAMT || 
                    record.DSPVCHDRAMT || 
                    record.LVSUBDRTOTAL || 
                    record.DEBITAMOUNT
                ),
                credit_amount: parseAmount(
                    record.CRAMT || 
                    record.DSPVCHCRAMT || 
                    record.LVSUBCRTOTAL || 
                    record.CREDITAMOUNT
                )
            };
            
            // Only add if we have at least a date or amounts
            if (daybookEntry.date || daybookEntry.debit_amount > 0 || daybookEntry.credit_amount > 0) {
                daybookEntries.push(daybookEntry);
            }
            
            // Progress indicator
            if ((i + 1) % 1000 === 0) {
                console.log(`🔄 Converted ${i + 1}/${records.length} daybook records`);
            }
            
        } catch (error) {
            if (config.debug) {
                console.error(`Error converting daybook record ${i + 1}:`, error.message);
            }
        }
    }
    
    console.log(`✅ Converted ${daybookEntries.length} daybook entries`);
    return daybookEntries;
}

// Function to parse amount from string
function parseAmount(amountStr) {
    if (!amountStr || amountStr.trim() === '') return 0.00;
    
    try {
        // Remove currency symbols and non-numeric characters except decimal point and minus
        const cleanAmount = amountStr.toString().replace(/[^0-9.-]/g, '');
        const amount = parseFloat(cleanAmount) || 0;
        
        return Math.abs(amount); // Store as positive, let debit/credit field indicate direction
    } catch (error) {
        return 0.00;
    }
}

// Function to format date from various formats to MySQL format
function formatDate(dateStr) {
    if (!dateStr || dateStr.trim() === '') return null;
    
    try {
        const cleanDate = dateStr.toString().trim();
        
        // Already in YYYY-MM-DD format
        if (/^\d{4}-\d{2}-\d{2}$/.test(cleanDate)) {
            return cleanDate;
        }
        
        // DD-MMM-YY format (like 10-Aug-24)
        if (/^\d{1,2}-[A-Za-z]{3}-\d{2}$/.test(cleanDate)) {
            const [day, month, year] = cleanDate.split('-');
            const monthMap = {
                'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
            };
            const fullYear = parseInt(year) > 50 ? '19' + year : '20' + year;
            const monthNum = monthMap[month] || '01';
            return `${fullYear}-${monthNum}-${day.padStart(2, '0')}`;
        }
        
        // DD-MMM-YYYY format (like 10-Aug-2024)
        if (/^\d{1,2}-[A-Za-z]{3}-\d{4}$/.test(cleanDate)) {
            const [day, month, year] = cleanDate.split('-');
            const monthMap = {
                'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
            };
            const monthNum = monthMap[month] || '01';
            return `${year}-${monthNum}-${day.padStart(2, '0')}`;
        }
        
        // DD/MM/YYYY format
        if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(cleanDate)) {
            const [day, month, year] = cleanDate.split('/');
            return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
        }
        
        // DD/MM/YY format
        if (/^\d{1,2}\/\d{1,2}\/\d{2}$/.test(cleanDate)) {
            const [day, month, year] = cleanDate.split('/');
            const fullYear = parseInt(year) > 50 ? '19' + year : '20' + year;
            return `${fullYear}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
        }
        
        // Try to parse as regular date
        const date = new Date(cleanDate);
        if (!isNaN(date.getTime())) {
            return date.toISOString().split('T')[0];
        }
        
        return null;
    } catch (error) {
        return null;
    }
}

// Function to map a parsed record to the full DaybookStockData table columns (51 columns, in order)
function mapRecordToDaybookStockDataColumns(record) {
    return [
        record.DATA_ROWS || null,
        record.ROW || null,
        record.ROW_NUMBER || null,
        record.VOUCHER_NUMBER || null,
        record.DATE || null,
        record.DATE_CHANGED || null,
        record.VCHTYPE || null,
        record.VCHNO || null,
        record.VOUCHER_GUID || null,
        record.LEDGER_NAME || null,
        record.VOUCHER_AMOUNT || null,
        record.ROUND_OFF || null,
        record.CREDIT_AMOUNT || null,
        record.DEBIT_AMOUNT || null,
        record.LEDGER_ADDRESS || null,
        record.LEDGER_CITY || null,
        record.LEDGER_PINCODE || null,
        record.LEDGER_STATE || null,
        record.LEDGER_COUNTRY || null,
        record.PARENT_GROUP || null,
        record.GST_REGISTRATION || null,
        record.OPENING_BALANCE_LEDGER || null,
        record.CLOSING_BALANCE || null,
        record.NARRATION || null,
        record.STOCK_ITEM_NUMBER || null,
        record.STOCK_GUID || null,
        record.STOCK_ITEM_NAME || null,
        record.STOCK_DATE_CONTEXT || null,
        record.STOCK_RATE || null,
        record.STOCK_ACTUAL_QTY || null,
        record.STOCK_AMOUNT || null,
        record.STOCK_BASE_UNITS || null,
        record.STOCK_BILLED_QTY || null,
        record.STOCK_DISCOUNT || null,
        record.HAS_STOCK_DATA || null,
        record.NO_STOCK_REASON || null,
        record.OPENING_QTY || null,
        record.OPENING_VALUE || null,
        record.INWARDS_QTY || null,
        record.INWARDS_VALUE || null,
        record.INWARDS_RATE || null,
        record.OUTWARDS_QTY || null,
        record.OUTWARDS_VALUE || null,
        record.OUTWARDS_RATE || null,
        record.CLOSING_QTY || null,
        record.CLOSING_VALUE || null,
        record.NET_CHANGE_QTY || null,
        record.NET_CHANGE_VALUE || null,
        record.MAPPING_METHOD || null,
        record.GST_TYPE || null,
        record.PARTY_GSTIN || null
    ];
}

// Main converter function
async function convertTallyDaybookXMLToMySQL() {
    let connection = null;
    
    try {
        // Step 1: Connect to database
        console.log('🔌 Connecting to database...');
        connection = await mysql.createConnection(config.db);
        console.log('✅ Database connected');

        // Step 2: Process Daybook Data
        console.log('\n🎯 Processing DAYBOOK DATA...');
        if (fs.existsSync(config.daybookXmlPath)) {
            console.log('📁 Daybook XML file found');
            
            // Read and parse daybook XML
            const daybookXmlContent = readXMLFile(config.daybookXmlPath);
            const daybookRecords = parseAllDaybookRecords(daybookXmlContent);
            
            if (daybookRecords.length > 0) {
                const daybookEntries = convertToDaybookEntries(daybookRecords);
                
                // Clear existing daybook data
                console.log('🗑️  Clearing existing daybook data...');
                await connection.execute('DELETE FROM DaybookStockData');
                
                // Insert daybook entries
                console.log('💾 Inserting daybook entries into database...');
                let successCount = 0;
                const insertQuery = `
                    INSERT INTO DaybookStockData (
                        \`DATA_ROWS\`, \`ROW\`, \`ROW_NUMBER\`, \`VOUCHER_NUMBER\`, \`DATE\`, \`DATE_CHANGED\`, \`VCHTYPE\`, \`VCHNO\`, \`VOUCHER_GUID\`, \`LEDGER_NAME\`, \`VOUCHER_AMOUNT\`,
                        \`ROUND_OFF\`, \`CREDIT_AMOUNT\`, \`DEBIT_AMOUNT\`, \`LEDGER_ADDRESS\`, \`LEDGER_CITY\`, \`LEDGER_PINCODE\`, \`LEDGER_STATE\`, \`LEDGER_COUNTRY\`, \`PARENT_GROUP\`,
                        \`GST_REGISTRATION\`, \`OPENING_BALANCE_LEDGER\`, \`CLOSING_BALANCE\`, \`NARRATION\`, \`STOCK_ITEM_NUMBER\`, \`STOCK_GUID\`, \`STOCK_ITEM_NAME\`, \`STOCK_DATE_CONTEXT\`,
                        \`STOCK_RATE\`, \`STOCK_ACTUAL_QTY\`, \`STOCK_AMOUNT\`, \`STOCK_BASE_UNITS\`, \`STOCK_BILLED_QTY\`, \`STOCK_DISCOUNT\`, \`HAS_STOCK_DATA\`, \`NO_STOCK_REASON\`,
                        \`OPENING_QTY\`, \`OPENING_VALUE\`, \`INWARDS_QTY\`, \`INWARDS_VALUE\`, \`INWARDS_RATE\`, \`OUTWARDS_QTY\`, \`OUTWARDS_VALUE\`, \`OUTWARDS_RATE\`, \`CLOSING_QTY\`,
                        \`CLOSING_VALUE\`, \`NET_CHANGE_QTY\`, \`NET_CHANGE_VALUE\`, \`MAPPING_METHOD\`, \`GST_TYPE\`, \`PARTY_GSTIN\`
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                `;
                for (const record of daybookRecords) {
                    try {
                        await connection.execute(insertQuery, mapRecordToDaybookStockDataColumns(record));
                        successCount++;
                    } catch (error) {
                        if (config.debug) {
                            console.log(`❌ Error inserting DaybookStockData entry: ${error.message}`);
                        }
                    }
                }
                console.log(`✅ Successfully inserted: ${successCount} DaybookStockData records`);
            } else {
                console.log('⚠️  No daybook records found');
            }
        } else {
            console.log('⚠️  Daybook XML file not found at specified path');
        }

        // Step 3: Final Summary and Verification
        console.log('\n🎉 Conversion completed!');
        
        // Verify daybook data
        const [daybookRows] = await connection.execute('SELECT COUNT(*) as total FROM DaybookStockData');
        console.log(`📋 Total daybook records in database: ${daybookRows[0].total}`);
        
        // Show sample daybook data
        if (daybookRows[0].total > 0) {
            const [sampleDaybook] = await connection.execute('SELECT * FROM DaybookStockData LIMIT 5');
            console.log('\n📝 Sample daybook records:');
            sampleDaybook.forEach((row, index) => {
                console.log(`${index + 1}. Date: ${row.date}, Type: ${row.vch_type}, No: ${row.vch_no}, Debit: ${row.debit_amount}, Credit: ${row.credit_amount}`);
            });
        }

        // Show statistics
        if (daybookRows[0].total > 0) {
            const [stats] = await connection.execute(`
                SELECT 
                    SUM(debit_amount) as total_debit,
                    SUM(credit_amount) as total_credit,
                    COUNT(DISTINCT vch_type) as voucher_types,
                    COUNT(DISTINCT DATE(date)) as unique_dates
                FROM DaybookStockData
            `);
            
            console.log('\n📊 Daybook Statistics:');
            console.log(`💰 Total Debit Amount: ${stats[0].total_debit || 0}`);
            console.log(`💰 Total Credit Amount: ${stats[0].total_credit || 0}`);
            console.log(`📝 Different Voucher Types: ${stats[0].voucher_types || 0}`);
            console.log(`📅 Unique Dates: ${stats[0].unique_dates || 0}`);
        }

    } catch (error) {
        console.error('❌ Error:', error.message);
        console.error('Stack:', error.stack);
        process.exit(1);
    } finally {
        if (connection) {
            await connection.end();
            console.log('🔌 Database connection closed');
        }
    }
}

// Run the converter
convertTallyDaybookXMLToMySQL();