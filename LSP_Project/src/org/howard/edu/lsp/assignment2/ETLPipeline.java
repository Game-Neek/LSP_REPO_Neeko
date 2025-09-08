package org.howard.edu.lsp.assignment2;
import java.io.BufferedReader;   // For efficient reading of text files line by line
import java.io.BufferedWriter;   // For efficient writing to text files
import java.io.File;             // Represents file paths
import java.io.FileReader;       // Reads characters from a file
import java.io.FileWriter;       // Writes characters to a file
import java.io.IOException;      // For handling file I/O errors
import java.io.PrintWriter;      // Easier text output (println, printf, etc.)
import java.math.BigDecimal;     // For exact decimal math (important for prices)
import java.math.RoundingMode;   // For specifying HALF_UP rounding
import java.nio.file.Files;      // Utility for creating directories if they don’t exist
import java.nio.file.Path;       // Represents directory/file paths
import java.util.Locale;         // For consistent UPPERCASE conversion

public class ETLPipeline {

    // === File paths for input and output ===
    private static final String INPUT_PATH  = "data/products.csv";                 // Where the input file is
    private static final String OUTPUT_PATH = "data/products_transformed.csv";     // Where the transformed file will go

    // Header row we ALWAYS write to the output file
    private static final String OUTPUT_HEADER = "ProductID,Name,Price,Category,PriceRange";

    // Discount for Electronics (10%)
    private static final BigDecimal ELECTRONICS_DISCOUNT = new BigDecimal("0.10");

    public static void main(String[] args) {
        // Counters for summary reporting
        int rowsRead = 0;         // number of data rows (not counting header)
        int rowsTransformed = 0;  // number of rows successfully transformed
        int rowsSkipped = 0;      // rows skipped due to bad format or errors
        boolean wroteOutput = false; // whether we successfully wrote an output file

        // --- Step 1: Make sure output directory exists ---
        try {
            Path outDir = Path.of(OUTPUT_PATH).getParent();  // Find the folder part of the output path
            if (outDir != null) {
                Files.createDirectories(outDir);            // Create folder(s) if missing
            }
        } catch (IOException e) {
            System.err.println("ERROR: Could not create output directory: " + e.getMessage());
            printSummary(rowsRead, rowsTransformed, rowsSkipped, OUTPUT_PATH, false);
            return; // Stop program if we can’t prepare the folder
        }

        // --- Step 2: Check input file exists ---
        File inFile = new File(INPUT_PATH);
        if (!inFile.exists() || !inFile.isFile()) {
            System.err.println("ERROR: Missing input file at '" + INPUT_PATH + "'.");
            printSummary(rowsRead, rowsTransformed, rowsSkipped, OUTPUT_PATH, false);
            return; // Stop program if no input file
        }

        // --- Step 3: Open input and output files safely ---
        try (
            BufferedReader br = new BufferedReader(new FileReader(inFile)); // Reader for input
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(OUTPUT_PATH))) // Writer for output
        ) {
            wroteOutput = true; // We successfully opened the output file

            // Read the first line of input (the header row)
            String headerLine = br.readLine();

            // Always write the required output header, regardless of input
            out.println(OUTPUT_HEADER);

            // If input was completely empty (no header), stop here
            if (headerLine == null) {
                printSummary(rowsRead, rowsTransformed, rowsSkipped, OUTPUT_PATH, true);
                return;
            }

            // --- Step 4: Process each data line after the header ---
            String line;
            while ((line = br.readLine()) != null) {
                rowsRead++; // Count how many lines we tried to process

                // Skip empty/blank lines
                if (line.isBlank()) {
                    rowsSkipped++;
                    continue;
                }

                // Split the line by commas into fields (no quotes/commas in fields per spec)
                String[] parts = line.split(",", -1); // -1 means keep empty fields if any
                if (parts.length != 4) {              // We expect exactly 4 fields
                    rowsSkipped++;
                    continue;
                }

                // Extract fields (trim spaces just in case)
                String idStr        = parts[0].trim(); // ProductID
                String name         = parts[1].trim(); // Name
                String priceStr     = parts[2].trim(); // Price
                String categoryOrig = parts[3].trim(); // Category (keep original for logic)

                // Parse numeric fields safely
                Integer productId;
                BigDecimal price;
                try {
                    productId = Integer.valueOf(idStr);     // ProductID → integer
                    price = new BigDecimal(priceStr);       // Price → BigDecimal (accurate)
                } catch (NumberFormatException ex) {
                    rowsSkipped++;                          // If parsing fails, skip this row
                    continue;
                }

                // --- Transformations (in exact order) ---

                // (1) NAME → uppercase
                String nameUpper = name.toUpperCase(Locale.ROOT);

                // (2) DISCOUNT if category is Electronics → subtract 10% and round to 2 decimals
                BigDecimal finalPrice = price; // Start with original
                if ("Electronics".equalsIgnoreCase(categoryOrig)) {
                    finalPrice = price.subtract(price.multiply(ELECTRONICS_DISCOUNT));
                }
                finalPrice = finalPrice.setScale(2, RoundingMode.HALF_UP); // Round 2 decimals, HALF_UP

                // (3) CATEGORY recategorization:
                // If discounted price > 500.00 AND original category == Electronics → Premium Electronics
                String finalCategory = categoryOrig;
                if ("Electronics".equalsIgnoreCase(categoryOrig)
                        && finalPrice.compareTo(new BigDecimal("500.00")) > 0) {
                    finalCategory = "Premium Electronics";
                }

                // (4) PRICE RANGE → based on final price
                String priceRange = computePriceRange(finalPrice);

                // --- Step 5: Write the transformed row to output file ---
                out.printf("%d,%s,%s,%s,%s%n",
                        productId,
                        nameUpper,
                        finalPrice.toPlainString(), // Plain string, no scientific notation
                        finalCategory,
                        priceRange
                );

                // Also print the row to the console so we can see progress
                System.out.printf("ProductID: %d | Name: %s | Price: $%s | Category: %s | PriceRange: %s%n",
                        productId, nameUpper, finalPrice.toPlainString(), finalCategory, priceRange);

                rowsTransformed++; // Successfully processed this row
            }

            // --- Step 6: Print final summary ---
            printSummary(rowsRead, rowsTransformed, rowsSkipped, OUTPUT_PATH, true);

        } catch (IOException e) {
            // Catch any file reading/writing errors
            System.err.println("I/O ERROR: " + e.getMessage());
            printSummary(rowsRead, rowsTransformed, rowsSkipped, OUTPUT_PATH, wroteOutput);
        }
    }

    // Decide PriceRange based on final price
    private static String computePriceRange(BigDecimal price) {
        BigDecimal ten      = new BigDecimal("10.00");
        BigDecimal hundred  = new BigDecimal("100.00");
        BigDecimal fiveHund = new BigDecimal("500.00");

        if (price.compareTo(ten) <= 0) {
            return "Low"; // 0.00 – 10.00
        }
        if (price.compareTo(ten) > 0 && price.compareTo(hundred) <= 0) {
            return "Medium"; // 10.01 – 100.00
        }
        if (price.compareTo(hundred) > 0 && price.compareTo(fiveHund) <= 0) {
            return "High"; // 100.01 – 500.00
        }
        return "Premium"; // 500.01 and above
    }

    // Helper method to print the run summary at the end
    private static void printSummary(int rowsRead, int rowsTransformed, int rowsSkipped,
                                     String outputPath, boolean wroteOutput) {
        System.out.println("---- Run Summary ----");
        System.out.println("Rows read       : " + rowsRead);
        System.out.println("Rows transformed: " + rowsTransformed);
        System.out.println("Rows skipped    : " + rowsSkipped);
        System.out.println("Output written  : " + (wroteOutput ? outputPath : "(none)"));
    }
}
