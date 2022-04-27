/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.text.StringEscapeUtils;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;
import org.junit.Assert;

import static org.apache.nifi.processors.standard.SplitContent.FRAGMENT_COUNT;
import static org.apache.nifi.processors.standard.SplitContent.FRAGMENT_ID;
import static org.apache.nifi.processors.standard.SplitContent.FRAGMENT_INDEX;
import static org.apache.nifi.processors.standard.SplitContent.SEGMENT_ORIGINAL_FILENAME;
import org.apache.nifi.util.StringUtils;

public class TestSplitContent {

    @Test
    public void testTextFormatLeadingPosition() {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.FORMAT, SplitContent.UTF8_FORMAT.getValue());
        runner.setProperty(SplitContent.BYTE_SEQUENCE, "ub");
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE_LOCATION, SplitContent.LEADING_POSITION.getValue());

        runner.enqueue("rub-a-dub-dub".getBytes());
        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "4");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 4);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        splits.get(0).assertContentEquals("r");
        splits.get(1).assertContentEquals("ub-a-d");
        splits.get(2).assertContentEquals("ub-d");
        splits.get(3).assertContentEquals("ub");
    }

    @Test
    public void testTextFormatSplits() {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.FORMAT, SplitContent.UTF8_FORMAT.getValue());
        runner.setProperty(SplitContent.BYTE_SEQUENCE, "test");
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE_LOCATION, SplitContent.LEADING_POSITION.getValue());

        final byte[] input = "This is a test. This is another test. And this is yet another test. Finally this is the last Test.".getBytes();
        runner.enqueue(input);
        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "4");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 4);

        runner.assertQueueEmpty();
        List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        splits.get(0).assertContentEquals("This is a ");
        splits.get(1).assertContentEquals("test. This is another ");
        splits.get(2).assertContentEquals("test. And this is yet another ");
        splits.get(3).assertContentEquals("test. Finally this is the last Test.");
        runner.clearTransferState();

        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.enqueue(input);
        runner.run();
        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "4");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 4);
        splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        splits.get(0).assertContentEquals("This is a ");
        splits.get(1).assertContentEquals(". This is another ");
        splits.get(2).assertContentEquals(". And this is yet another ");
        splits.get(3).assertContentEquals(". Finally this is the last Test.");
        runner.clearTransferState();

        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE_LOCATION, SplitContent.TRAILING_POSITION.getValue());
        runner.enqueue(input);
        runner.run();
        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "4");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 4);
        splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        splits.get(0).assertContentEquals("This is a test");
        splits.get(1).assertContentEquals(". This is another test");
        splits.get(2).assertContentEquals(". And this is yet another test");
        splits.get(3).assertContentEquals(". Finally this is the last Test.");
        runner.clearTransferState();

        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE_LOCATION, SplitContent.TRAILING_POSITION.getValue());
        runner.enqueue("This is a test. This is another test. And this is yet another test. Finally this is the last test".getBytes());
        runner.run();
        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "4");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 4);
        splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        splits.get(0).assertContentEquals("This is a test");
        splits.get(1).assertContentEquals(". This is another test");
        splits.get(2).assertContentEquals(". And this is yet another test");
        splits.get(3).assertContentEquals(". Finally this is the last test");
        runner.clearTransferState();

        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE_LOCATION, SplitContent.LEADING_POSITION.getValue());
        runner.enqueue("This is a test. This is another test. And this is yet another test. Finally this is the last test".getBytes());
        runner.run();
        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "5");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 5);
        splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        splits.get(0).assertContentEquals("This is a ");
        splits.get(1).assertContentEquals("test. This is another ");
        splits.get(2).assertContentEquals("test. And this is yet another ");
        splits.get(3).assertContentEquals("test. Finally this is the last ");
        splits.get(4).assertContentEquals("test");

        runner.clearTransferState();
    }

    @Test
    public void testTextFormatTrailingPosition() {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.FORMAT, SplitContent.UTF8_FORMAT.getValue());
        runner.setProperty(SplitContent.BYTE_SEQUENCE, "ub");
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE_LOCATION, SplitContent.TRAILING_POSITION.getValue());

        runner.enqueue("rub-a-dub-dub".getBytes());
        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "3");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 3);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        splits.get(0).assertContentEquals("rub");
        splits.get(1).assertContentEquals("-a-dub");
        splits.get(2).assertContentEquals("-dub");
    }

    @Test
    public void testSmallSplits() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "FFFF");

        runner.enqueue(new byte[]{1, 2, 3, 4, 5, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 5, 4, 3, 2, 1});
        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);
        final MockFlowFile split2 = splits.get(1);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4, 5});
        split2.assertContentEquals(new byte[]{(byte) 0xFF, 5, 4, 3, 2, 1});
    }

    @Test
    public void testWithSingleByteSplit() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "FF");

        runner.enqueue(new byte[]{1, 2, 3, 4, 5, (byte) 0xFF, 5, 4, 3, 2, 1});
        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);
        final MockFlowFile split2 = splits.get(1);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4, 5});
        split2.assertContentEquals(new byte[]{5, 4, 3, 2, 1});
    }

    @Test
    public void testWithLargerSplit() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "05050505");

        runner.enqueue(new byte[]{
            1, 2, 3, 4, 5,
            5, 5, 5, 5,
            5, 4, 3, 2, 1});

        runner.run();
        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        final MockFlowFile originalFlowFile = runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0);
        originalFlowFile.assertAttributeExists(FRAGMENT_ID);
        originalFlowFile.assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);
        final MockFlowFile split2 = splits.get(1);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4});
        split2.assertContentEquals(new byte[]{5, 5, 4, 3, 2, 1});
    }

    @Test
    public void testKeepingSequence() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "05050505");

        runner.enqueue(new byte[]{
            1, 2, 3, 4, 5,
            5, 5, 5, 5,
            5, 4, 3, 2, 1});

        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);
        final MockFlowFile split2 = splits.get(1);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4, 5, 5, 5, 5});
        split2.assertContentEquals(new byte[]{5, 5, 4, 3, 2, 1});
    }

    @Test
    public void testEndsWithSequence() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "05050505");

        runner.enqueue(new byte[]{1, 2, 3, 4, 5, 5, 5, 5});

        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 1);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4});
    }

    @Test
    public void testEndsWithSequenceAndKeepSequence() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "05050505");

        runner.enqueue(new byte[]{1, 2, 3, 4, 5, 5, 5, 5});

        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 1);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4, 5, 5, 5, 5});
    }

    @Test
    public void testStartsWithSequence() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "05050505");

        runner.enqueue(new byte[]{5, 5, 5, 5, 1, 2, 3, 4});

        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 1);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4});
    }

    @Test
    public void testStartsWithSequenceAndKeepSequence() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "05050505");

        runner.enqueue(new byte[]{5, 5, 5, 5, 1, 2, 3, 4});

        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        splits.get(0).assertContentEquals(new byte[]{5, 5, 5, 5});
        splits.get(1).assertContentEquals(new byte[]{1, 2, 3, 4});
    }

    @Test
    public void testRegexFormattedSequenceValidation() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.FORMAT, SplitContent.REGEX_FORMAT.getValue());

        runner.setProperty(SplitContent.BYTE_SEQUENCE, "[\\n\\r+"); // missing ] symbol
        runner.assertNotValid();

        runner.setProperty(SplitContent.BYTE_SEQUENCE, "[\\n\\r]+"); // corrected the regex error
        runner.assertValid();
    }

    @Test
    public void testRegexSearch () throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.BUFFER_SIZE, "1 KB");
        runner.setProperty(SplitContent.FORMAT, SplitContent.REGEX_FORMAT.getValue());

        runner.setProperty(SplitContent.BYTE_SEQUENCE, "[\\n\\r]+");
        
        String splitStrings[] = {"\u0394: This is line one.", "\u0395. Line 2.", "\u0396: And this is line three!"};
        String delimStrings[] = {"\r\n\r\n", "\n\n\n", "\r\r\n"};
        String contentStr = "";
        for (int i = 0; i < splitStrings.length; i++)
            contentStr += splitStrings[i] + delimStrings[i];
        runner.enqueue(contentStr);

        runner.run();
        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        
        for (int i = 0; i < splits.size(); i++) {
            MockFlowFile split = splits.get(i);
//runner.getLogger().info("Split: '" + StringEscapeUtils.escapeJava(split.getContent()) + "'   Original: '" + StringEscapeUtils.escapeJava(splitStrings[i]) + "'");
            split.assertContentEquals(splitStrings[i].getBytes());
        }
    }


    @Test
    public void testRegexTrailingSearch () throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
    
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE_LOCATION, SplitContent.TRAILING_POSITION.getValue());

        runner.setProperty(SplitContent.BUFFER_SIZE, "1 KB");
        runner.setProperty(SplitContent.FORMAT, SplitContent.REGEX_FORMAT.getValue());

        runner.setProperty(SplitContent.BYTE_SEQUENCE, "[\\n\\r]+");
        
        String splitStrings[] = {"\u0394: This is line one.", "\u0395. Line 2.", "\u0396: And this is line three!"};
        String delimStrings[] = {"\r\n\r\n", "\n\n\n", "\r\r\n"};
        String contentStr = "";
        for (int i = 0; i < splitStrings.length; i++)
            contentStr += splitStrings[i] + delimStrings[i];
        runner.enqueue(contentStr);

        runner.run();
        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        
        for (int i = 0; i < splits.size(); i++) {
            MockFlowFile split = splits.get(i);
//runner.getLogger().info("Split: '" + StringEscapeUtils.escapeJava(split.getContent()) + "'   Original: '" + StringEscapeUtils.escapeJava(splitStrings[i] + delimStrings[i]) + "'");
            split.assertContentEquals((splitStrings[i] + delimStrings[i]).getBytes());
        }
    }

    @Test
    public void testRegexLeadingSearch () throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
    
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE_LOCATION, SplitContent.LEADING_POSITION.getValue());

        runner.setProperty(SplitContent.BUFFER_SIZE, "1 KB");
        runner.setProperty(SplitContent.FORMAT, SplitContent.REGEX_FORMAT.getValue());

        runner.setProperty(SplitContent.BYTE_SEQUENCE, "[\\n\\r]+");
        
        String splitStrings[] = {"\u0394: This is line one.", "\u0395. Line 2.", "\u0396: And this is line three!"};
        String delimStrings[] = {"\r\n\r\n", "\n\n\n", "\r\r\n"};
        String contentStr = "";
        for (int i = 0; i < splitStrings.length; i++)
            contentStr += delimStrings[i] + splitStrings[i];
        runner.enqueue(contentStr);

        runner.run();
        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        
        for (int i = 0; i < splits.size(); i++) {
            MockFlowFile split = splits.get(i);
//runner.getLogger().info("Split: '" + StringEscapeUtils.escapeJava(split.getContent()) + "'   Original: '" + StringEscapeUtils.escapeJava(delimStrings[i] + splitStrings[i]) + "'");
            split.assertContentEquals((delimStrings[i] + splitStrings[i]).getBytes());
        }
    }

    @Test
    public void testRegexMultiReadSearch () throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        // small buffer size
        runner.setProperty(SplitContent.BUFFER_SIZE, "16B");
        runner.setProperty(SplitContent.FORMAT, SplitContent.REGEX_FORMAT.getValue());

        runner.setProperty(SplitContent.BYTE_SEQUENCE, "[\\n\\r]+");
        
        String splitStrings[] = {"\u0394: This is line one.", "\u0395. Line 2.", "\u0396: And this is a very long (relative to the buffer size) line three!"};
        String delimStrings[] = {"\r\n\r\n", "\n\n\n", "\r\r\n"};
        String contentStr = "";
        for (int i = 0; i < splitStrings.length; i++)
            contentStr += splitStrings[i] + delimStrings[i];
        runner.enqueue(contentStr);

        runner.run();
        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        
        for (int i = 0; i < splits.size(); i++) {
            MockFlowFile split = splits.get(i);
//runner.getLogger().info("Split: '" + StringEscapeUtils.escapeJava(split.getContent()) + "'   Original: '" + StringEscapeUtils.escapeJava(splitStrings[i]) + "'");
            split.assertContentEquals(splitStrings[i].getBytes());
        }
    }

    
    @Test
    public void testRegexLargeMultiReadSearch () throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.FORMAT, SplitContent.REGEX_FORMAT.getValue());
        runner.setProperty(SplitContent.BUFFER_SIZE, "64KB");

        runner.setProperty(SplitContent.BYTE_SEQUENCE, "\\n+");
        final int totalSize = 10 * 1024 * 1024;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int splitCount = 0;
        List<Integer> splitSizes = new ArrayList<>();
        
        while (baos.size() < totalSize - 2048) {
            int splitSize = (int)Math.ceil(2000 * Math.random());
            int delimSize = (int)Math.ceil(48 * Math.random());
            String split = StringUtils.leftPad("", splitSize, 'X');
            String delim = StringUtils.leftPad("", delimSize, '\n');
            byte splitBytes[] = split.getBytes();
            byte delimBytes[] = delim.getBytes();
            baos.write(splitBytes);
            baos.write(delimBytes);

            splitSizes.add(splitBytes.length);
        }
        
        runner.enqueue(new ByteArrayInputStream(baos.toByteArray(), 0, baos.size()));

        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(SplitContent.REL_SPLITS, splitSizes.size());

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);        
        for (int i = 0; i < splits.size(); i++) {
            MockFlowFile split = splits.get(i);
            // compare the length of the output to the length of the input
            Assert.assertEquals("Split["  + i + "/" + splits.size() + "] length mismatch", (long)splitSizes.get(i), (long)split.getData().length);
        }
    }

    @Test
    public void testRegexMidPatternTrailingSearch () throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE_LOCATION, SplitContent.TRAILING_POSITION.getValue());
        runner.setProperty(SplitContent.FORMAT, SplitContent.REGEX_FORMAT.getValue());

        runner.setProperty(SplitContent.BYTE_SEQUENCE, "[\\n\\r]+");
        
        String splitStrings[] = {"\u0394: This is line one.", "\u0395. Line 2.", "\u0396: And this is a very long (relative to the buffer size) line three!"};

        // buffer size that overlaps regex.  The "\r" will match the delimiter regex, which would give a false split
        runner.setProperty(SplitContent.BUFFER_SIZE, splitStrings[0].length() + 1 + "B");


        String delimStrings[] = {"\r\n\r\n", "\n\n\n", "\r\r\n"};
        String contentStr = "";
        for (int i = 0; i < splitStrings.length; i++)
            contentStr += splitStrings[i] + delimStrings[i];
        runner.enqueue(contentStr);

        runner.run();
        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        
        for (int i = 0; i < splits.size(); i++) {
            MockFlowFile split = splits.get(i);
            split.assertContentEquals((splitStrings[i] + delimStrings[i]).getBytes());
        }
    }


    @Test
    public void testSmallSplitsThenMerge() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "FFFF");

        runner.enqueue(new byte[]{1, 2, 3, 4, 5, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 5, 4, 3, 2, 1});
        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.assertTransferCount(SplitContent.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);
        final MockFlowFile split2 = splits.get(1);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4, 5, (byte) 0xFF, (byte) 0xFF});
        split2.assertContentEquals(new byte[]{(byte) 0xFF, 5, 4, 3, 2, 1});

        final TestRunner mergeRunner = TestRunners.newTestRunner(new MergeContent());
        mergeRunner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        mergeRunner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        mergeRunner.enqueue(splits.toArray(new MockFlowFile[0]));
        mergeRunner.run();

        mergeRunner.assertTransferCount(MergeContent.REL_MERGED, 1);
        mergeRunner.assertTransferCount(MergeContent.REL_ORIGINAL, 2);
        mergeRunner.assertTransferCount(MergeContent.REL_FAILURE, 0);

        final List<MockFlowFile> packed = mergeRunner.getFlowFilesForRelationship(MergeContent.REL_MERGED);
        packed.get(0).assertContentEquals(new byte[]{1, 2, 3, 4, 5, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 5, 4, 3, 2, 1});
    }

    @Test
    public void testNoSplitterInString() {

        String content = "UVAT";

        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.FORMAT, SplitContent.UTF8_FORMAT.getValue());
        runner.setProperty(SplitContent.BYTE_SEQUENCE, ",");
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.BYTE_SEQUENCE_LOCATION, SplitContent.TRAILING_POSITION.getValue());

        runner.enqueue(content.getBytes());
        runner.run();

        runner.assertTransferCount(SplitContent.REL_SPLITS, 1);
        MockFlowFile splitResult = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS).get(0);
        splitResult.assertAttributeExists(FRAGMENT_ID);
        splitResult.assertAttributeExists(SEGMENT_ORIGINAL_FILENAME);
        splitResult.assertAttributeEquals(FRAGMENT_COUNT, "1");
        splitResult.assertAttributeEquals(FRAGMENT_INDEX, "1");
        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        splits.get(0).assertContentEquals(content);
    }
}
