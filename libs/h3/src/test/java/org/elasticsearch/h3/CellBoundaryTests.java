/*
 * Based on the h3 project by Uber (@uber)
 * https://github.com/uber/h3
 * Licensed to Elasticsearch B.V under the Apache 2.0 License.
 * Elasticsearch B.V licenses this file, including any modifications, to you under the Apache 2.0 License.
 * See the LICENSE file in the project root for more information.
 */
package org.elasticsearch.h3;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.elasticsearch.test.ESTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class CellBoundaryTests extends ESTestCase {

    public void testRes0() throws Exception {
        processFile("res00cells.txt");
    }

    public void testRes1() throws Exception {
        processFile("res01cells.txt");
    }

    public void testRes2() throws Exception {
        processFile("res02cells.txt");
    }

    public void testRes3() throws Exception {
        processFile("res03cells.txt");
    }

    public void testBc05r08cells() throws Exception {
        processFile("bc05r08cells.txt");
    }

    public void testBc05r09cells() throws Exception {
        processFile("bc05r09cells.txt");
    }

    public void testBc05r10cells() throws Exception {
        processFile("bc05r10cells.txt");
    }

    public void testBc05r11cells() throws Exception {
        processFile("bc05r11cells.txt");
    }

    public void testBc05r12cells() throws Exception {
        processFile("bc05r12cells.txt");
    }

    public void testBc05r13cells() throws Exception {
        processFile("bc05r13cells.txt");
    }

    public void testBc05r05cells() throws Exception {
        processFile("bc05r14cells.txt");
    }

    public void testBc05r15cells() throws Exception {
        processFile("bc05r15cells.txt");
    }

    public void testBc14r08cells() throws Exception {
        processFile("bc14r08cells.txt");
    }

    public void testBc14r09cells() throws Exception {
        processFile("bc14r09cells.txt");
    }

    public void testBc14r10cells() throws Exception {
        processFile("bc14r10cells.txt");
    }

    public void testBc14r11cells() throws Exception {
        processFile("bc14r11cells.txt");
    }

    public void testBc14r12cells() throws Exception {
        processFile("bc14r12cells.txt");
    }

    public void testBc14r13cells() throws Exception {
        processFile("bc14r13cells.txt");
    }

    public void testBc14r14cells() throws Exception {
        processFile("bc14r14cells.txt");
    }

    public void testBc14r15cells() throws Exception {
        processFile("bc14r15cells.txt");
    }

    public void testBc19r08cells() throws Exception {
        processFile("bc19r08cells.txt");
    }

    public void testBc19r09cells() throws Exception {
        processFile("bc19r09cells.txt");
    }

    public void testBc19r10cells() throws Exception {
        processFile("bc19r10cells.txt");
    }

    public void testBc19r11cells() throws Exception {
        processFile("bc19r11cells.txt");
    }

    public void testBc19r12cells() throws Exception {
        processFile("bc19r12cells.txt");
    }

    public void testBc19r13cells() throws Exception {
        processFile("bc19r13cells.txt");
    }

    public void testBc19r14cells() throws Exception {
        processFile("bc19r14cells.txt");
    }

    private void processFile(String file) throws IOException {
        InputStream fis = getClass().getResourceAsStream(file + ".gz");
        BufferedReader reader = new BufferedReader(new InputStreamReader(new GzipCompressorInputStream(fis), StandardCharsets.UTF_8));
        String h3Address = reader.readLine();
        while (h3Address != null) {
            assertEquals(true, H3.h3IsValid(h3Address));
            long h3 = H3.stringToH3(h3Address);
            assertEquals(true, H3.h3IsValid(h3));
            processOne(h3Address, reader);
            h3Address = reader.readLine();
        }
    }

    private void processOne(String h3Address, BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if ("{".equals(line) == false) {
            throw new IllegalArgumentException();
        }
        line = reader.readLine();
        List<double[]> points = new ArrayList<>();
        while ("}".equals(line) == false) {
            StringTokenizer tokens = new StringTokenizer(line, " ");
            assertEquals(2, tokens.countTokens());
            double lat = Double.parseDouble(tokens.nextToken());
            double lon = Double.parseDouble(tokens.nextToken());
            points.add(new double[] { lat, lon });
            line = reader.readLine();
        }
        CellBoundary boundary = H3.h3ToGeoBoundary(h3Address);
        assert boundary.numPoints() == points.size();
        for (int i = 0; i < boundary.numPoints(); i++) {
            assertEquals(h3Address, points.get(i)[0], boundary.getLatLon(i).getLatDeg(), 1e-8);
            assertEquals(h3Address, points.get(i)[1], boundary.getLatLon(i).getLonDeg(), 1e-8);
        }
    }
}
