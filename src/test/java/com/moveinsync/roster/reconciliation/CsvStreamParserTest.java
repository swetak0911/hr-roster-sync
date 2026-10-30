package com.moveinsync.roster.reconciliation;

import org.junit.jupiter.api.Test;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CsvStreamParserTest {

    @Test
    void testBasicCsvParsing() throws Exception {
        String csv = "employeeId,name,email\nE-1,Asha,asha@example.com\nE-2,Ravi,ravi@example.com\n";
        List<CsvStreamParser.ParsedRow> rows = new ArrayList<>();
        CsvStreamParser.parse(new StringReader(csv), rows::add);

        assertEquals(3, rows.size());
        assertEquals(List.of("employeeId", "name", "email"), rows.get(0).fields());
        assertEquals(List.of("E-1", "Asha", "asha@example.com"), rows.get(1).fields());
        assertEquals(List.of("E-2", "Ravi", "ravi@example.com"), rows.get(2).fields());
        assertFalse(rows.get(1).malformed());
        assertFalse(rows.get(2).malformed());
    }

    @Test
    void testQuotedFieldWithCommas() throws Exception {
        String csv = "employeeId,name,email\nE-1,\"Rao, Asha\",asha@example.com\n";
        List<CsvStreamParser.ParsedRow> rows = new ArrayList<>();
        CsvStreamParser.parse(new StringReader(csv), rows::add);

        assertEquals(2, rows.size());
        assertEquals("Rao, Asha", rows.get(1).fields().get(1));
        assertFalse(rows.get(1).malformed());
    }

    @Test
    void testEscapedDoubleQuotes() throws Exception {
        String csv = "employeeId,name,email\nE-1,\"Asha \"\"The Rock\"\" Rao\",asha@example.com\n";
        List<CsvStreamParser.ParsedRow> rows = new ArrayList<>();
        CsvStreamParser.parse(new StringReader(csv), rows::add);

        assertEquals(2, rows.size());
        assertEquals("Asha \"The Rock\" Rao", rows.get(1).fields().get(1));
        assertFalse(rows.get(1).malformed());
    }

    @Test
    void testMultiLineQuotedField() throws Exception {
        String csv = "employeeId,name,email\nE-1,\"Line 1\nLine 2\",asha@example.com\n";
        List<CsvStreamParser.ParsedRow> rows = new ArrayList<>();
        CsvStreamParser.parse(new StringReader(csv), rows::add);

        assertEquals(2, rows.size());
        assertEquals("Line 1\nLine 2", rows.get(1).fields().get(1));
        assertFalse(rows.get(1).malformed());
    }

    @Test
    void testUnclosedQuoteMarksMalformed() throws Exception {
        String csv = "employeeId,name,email\nE-1,\"Unclosed quote,asha@example.com";
        List<CsvStreamParser.ParsedRow> rows = new ArrayList<>();
        CsvStreamParser.parse(new StringReader(csv), rows::add);

        assertEquals(2, rows.size());
        assertTrue(rows.get(1).malformed());
    }

    @Test
    void testWindowsCrLfNewlines() throws Exception {
        String csv = "employeeId,name,email\r\nE-1,Asha,asha@example.com\r\nE-2,Ravi,ravi@example.com\r\n";
        List<CsvStreamParser.ParsedRow> rows = new ArrayList<>();
        CsvStreamParser.parse(new StringReader(csv), rows::add);

        assertEquals(3, rows.size());
        assertEquals(List.of("E-1", "Asha", "asha@example.com"), rows.get(1).fields());
        assertEquals(List.of("E-2", "Ravi", "ravi@example.com"), rows.get(2).fields());
    }

    @Test
    void testEmptyFieldsAndTrailingNewlines() throws Exception {
        String csv = "employeeId,name,email,managerId,officeCode,phone\nE-1,Asha,asha@example.com,,,+919876543210\n\n";
        List<CsvStreamParser.ParsedRow> rows = new ArrayList<>();
        CsvStreamParser.parse(new StringReader(csv), rows::add);

        assertEquals(2, rows.size());
        assertEquals(List.of("E-1", "Asha", "asha@example.com", "", "", "+919876543210"), rows.get(1).fields());
    }
}

