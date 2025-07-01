const fs = require('fs');
const path = require('path');

// Script to clean BOM from XML files
function cleanBOMFromFile(filePath) {
    try {
        console.log(`🔍 Reading file: ${filePath}`);
        
        // Read as buffer to handle encoding
        const buffer = fs.readFileSync(filePath);
        
        // Check for BOM patterns
        const hasBOM_UTF8 = buffer[0] === 0xEF && buffer[1] === 0xBB && buffer[2] === 0xBF;
        const hasBOM_UTF16BE = buffer[0] === 0xFE && buffer[1] === 0xFF;
        const hasBOM_UTF16LE = buffer[0] === 0xFF && buffer[1] === 0xFE;
        
        if (hasBOM_UTF8) {
            console.log('✅ Found UTF-8 BOM, removing...');
            const cleanBuffer = buffer.slice(3);
            const backupPath = filePath + '.backup';
            
            // Create backup
            fs.writeFileSync(backupPath, buffer);
            console.log(`💾 Backup created: ${backupPath}`);
            
            // Write clean file
            fs.writeFileSync(filePath, cleanBuffer);
            console.log('✅ BOM removed successfully');
            
        } else if (hasBOM_UTF16BE || hasBOM_UTF16LE) {
            console.log('✅ Found UTF-16 BOM, converting to UTF-8...');
            const content = buffer.toString('utf16le');
            const cleanContent = content.replace(/^\uFEFF/, '');
            
            const backupPath = filePath + '.backup';
            fs.writeFileSync(backupPath, buffer);
            console.log(`💾 Backup created: ${backupPath}`);
            
            fs.writeFileSync(filePath, cleanContent, 'utf8');
            console.log('✅ Converted to UTF-8 without BOM');
            
        } else {
            console.log('ℹ️  No BOM detected');
            
            // Check for other invisible characters
            const content = buffer.toString('utf8');
            const firstChar = content.charCodeAt(0);
            
            if (firstChar > 127 || firstChar < 32) {
                console.log(`⚠️  Suspicious first character detected: ${firstChar} ('${content[0]}')`);
                
                // Remove first character if it's not printable ASCII or common XML start
                if (content[0] !== '<' && firstChar !== 32 && firstChar !== 9 && firstChar !== 10 && firstChar !== 13) {
                    const cleanContent = content.substring(1);
                    const backupPath = filePath + '.backup';
                    
                    fs.writeFileSync(backupPath, buffer);
                    console.log(`💾 Backup created: ${backupPath}`);
                    
                    fs.writeFileSync(filePath, cleanContent, 'utf8');
                    console.log('✅ Removed suspicious first character');
                }
            }
        }
        
        // Final verification
        const finalContent = fs.readFileSync(filePath, 'utf8');
        console.log(`🔍 File now starts with: "${finalContent.substring(0, 50)}..."`);
        console.log(`🔍 First character code: ${finalContent.charCodeAt(0)}`);
        
    } catch (error) {
        console.error('❌ Error cleaning BOM:', error.message);
    }
}

// Get XML file path from environment or command line
const xmlPath = process.env.TALLY_XML_PATH || process.argv[2];

if (!xmlPath) {
    console.log('❌ Please provide XML file path:');
    console.log('   node bom-cleaner.js path/to/your/file.xml');
    console.log('   or set TALLY_XML_PATH environment variable');
    process.exit(1);
}

if (!fs.existsSync(xmlPath)) {
    console.log(`❌ File not found: ${xmlPath}`);
    process.exit(1);
}

console.log('🚀 Starting BOM cleaner...');
cleanBOMFromFile(xmlPath);
console.log('✅ Done!');