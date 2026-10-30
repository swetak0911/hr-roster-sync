package com.moveinsync.roster.reconciliation;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * High-performance streaming RFC-4180 compliant CSV parser.
 * Supports:
 * - Quoted fields containing commas
 * - Escaped double quotes ("")
 * - Multi-line quoted fields
 * - Malformed row detection (e.g. unclosed quotes)
 * - Trailing CRLF and empty line handling
 */
public class CsvStreamParser {

    public record ParsedRow(long rowNumber, List<String> fields, boolean malformed) {}

    @FunctionalInterface
    public interface RowHandler {
        void handleRow(ParsedRow row) throws IOException;
    }

    private static final int BUFFER_SIZE = 8192;

    public static void parse(Reader reader, RowHandler handler) throws IOException {
        char[] buf = new char[BUFFER_SIZE];
        int charsRead;

        List<String> currentFields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();

        boolean inQuotes = false;
        boolean afterQuote = false;
        boolean lastWasCr = false;
        long rowNumber = 1;
        boolean malformed = false;
        boolean hasRowData = false;

        while ((charsRead = reader.read(buf)) != -1) {
            for (int i = 0; i < charsRead; i++) {
                char c = buf[i];

                if (lastWasCr) {
                    lastWasCr = false;
                    if (c == '\n') {
                        // Consumed \r\n pair
                        continue;
                    }
                    // \r was standalone newline, now process current char c
                }

                if (inQuotes) {
                    if (c == '"') {
                        inQuotes = false;
                        afterQuote = true;
                    } else {
                        currentField.append(c);
                        hasRowData = true;
                    }
                } else if (afterQuote) {
                    if (c == '"') {
                        // Escaped quote: "" inside quoted field
                        currentField.append('"');
                        inQuotes = true;
                        afterQuote = false;
                        hasRowData = true;
                    } else if (c == ',') {
                        currentFields.add(currentField.toString());
                        currentField.setLength(0);
                        afterQuote = false;
                        hasRowData = true;
                    } else if (c == '\r' || c == '\n') {
                        currentFields.add(currentField.toString());
                        currentField.setLength(0);
                        afterQuote = false;
                        if (c == '\r') lastWasCr = true;
                        handler.handleRow(new ParsedRow(rowNumber, currentFields, malformed));
                        rowNumber++;
                        currentFields = new ArrayList<>();
                        malformed = false;
                        hasRowData = false;
                    } else if (Character.isWhitespace(c)) {
                        // Tolerate whitespace after closing quote
                    } else {
                        currentField.append(c);
                        malformed = true;
                        afterQuote = false;
                        hasRowData = true;
                    }
                } else {
                    if (c == '"') {
                        if (currentField.length() == 0) {
                            inQuotes = true;
                            hasRowData = true;
                        } else {
                            currentField.append('"');
                            malformed = true;
                            hasRowData = true;
                        }
                    } else if (c == ',') {
                        currentFields.add(currentField.toString());
                        currentField.setLength(0);
                        hasRowData = true;
                    } else if (c == '\r' || c == '\n') {
                        if (c == '\r') lastWasCr = true;
                        if (hasRowData || !currentFields.isEmpty() || currentField.length() > 0) {
                            currentFields.add(currentField.toString());
                            currentField.setLength(0);
                            handler.handleRow(new ParsedRow(rowNumber, currentFields, malformed));
                            rowNumber++;
                            currentFields = new ArrayList<>();
                            malformed = false;
                            hasRowData = false;
                        }
                    } else {
                        currentField.append(c);
                        hasRowData = true;
                    }
                }
            }
        }

        if (inQuotes) {
            malformed = true;
        }
        if (hasRowData || !currentFields.isEmpty() || currentField.length() > 0) {
            currentFields.add(currentField.toString());
            handler.handleRow(new ParsedRow(rowNumber, currentFields, malformed));
        }
    }
}
