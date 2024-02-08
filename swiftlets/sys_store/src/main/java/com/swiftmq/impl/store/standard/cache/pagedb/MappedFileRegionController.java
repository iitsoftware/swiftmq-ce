/*
 * Copyright 2024 IIT Software GmbH
 *
 * IIT Software GmbH licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.swiftmq.impl.store.standard.cache.pagedb;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.cache.Page;
import com.swiftmq.impl.store.standard.pagedb.PageSize;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public class MappedFileRegionController implements FileAccess {
    StoreContext ctx;
    private final String filePath;
    private final boolean freePoolEnabled;
    private final List<MappedFileRegion> regions;
    private final int pageSize;
    private final int targetRegionSizeInPages; // Target size of a region in pages
    private final PriorityQueue<Integer> freePagesQueue;
    private int totalNumberOfPages = 0;
    private long fileSize = 0;

    public MappedFileRegionController(StoreContext ctx, String filePath, int targetRegionSizeInPages, boolean freePoolEnabled) {
        this.ctx = ctx;
        this.filePath = filePath + File.separatorChar + PageDB.FILENAME;
        this.freePoolEnabled = freePoolEnabled;
        this.regions = new ArrayList<>();
        this.pageSize = PageSize.getCurrent();
        this.targetRegionSizeInPages = targetRegionSizeInPages;
        this.freePagesQueue = new PriorityQueue<>();
        new File(filePath).mkdirs();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/created");
    }

    private void createInitialRegions() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/createInitialRegions ...");
        try {
            File file = new File(filePath);
            fileSize = file.length();
            totalNumberOfPages = (int) (fileSize / pageSize);
            int currentStartPage = 0;

            while (currentStartPage < totalNumberOfPages) {
                int regionSizeInPages = Math.min(targetRegionSizeInPages, totalNumberOfPages - currentStartPage);
                regions.add(new MappedFileRegion(ctx, filePath, currentStartPage, (long) regionSizeInPages * pageSize));
                currentStartPage += regionSizeInPages;
            }
        } catch (IOException e) {
            throw new RuntimeException("Error initializing file regions", e);
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/createInitialRegions done");
    }

    private void scanAllRegionsForFreePages() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/scanAllRegionsForFreePages ...");

        List<Integer> allFreePages = regions.parallelStream()
                .map(MappedFileRegion::scanForFreePages)
                .flatMap(List::stream)
                .collect(Collectors.toList()); // Collect to a list first

        // Now we can add all collected free pages to the priority queue in a single thread
        freePagesQueue.addAll(allFreePages);

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/scanAllRegionsForFreePages done");
    }

    private MappedFileRegion findRegionForPage(int pageNo) throws IOException {
        int regionIndex = pageNo / targetRegionSizeInPages;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", this + "/findRegionForPage, pageNo=" + pageNo + ", regionIndex=" + regionIndex);
        if (regionIndex < regions.size()) {
            return regions.get(regionIndex);
        } else {
            // If the page is beyond the current limit, create a new region
            return createAndAddRegion(pageNo);
        }
    }

    private MappedFileRegion createAndAddRegion(int pageNo) throws IOException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", this + "/createAndAddRegion, pageNo=" + pageNo + " ...");
        int regionStartPage = (pageNo / targetRegionSizeInPages) * targetRegionSizeInPages;
        long regionSize = (long) targetRegionSizeInPages * pageSize;
        MappedFileRegion newRegion = new MappedFileRegion(ctx, filePath, regionStartPage, regionSize);
        freePagesQueue.addAll(newRegion.scanForFreePages());
        regions.add(newRegion);
        fileSize = fileSize + regionSize;
        totalNumberOfPages = regions.size() * targetRegionSizeInPages;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", this + "/createAndAddRegion, pageNo=" + pageNo + " done");
        return newRegion;
    }

    private void adjustFileSize(int newSize) throws IOException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/adjustFileSize, newSize=" + newSize);
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.setLength(newSize);
            fileSize = newSize;
        }
    }

    @Override
    public void init() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/init ...");
        createInitialRegions();
        if (freePoolEnabled)
            scanAllRegionsForFreePages();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/init done");
    }

    @Override
    public int numberPages() {
        return totalNumberOfPages;
    }

    @Override
    public int freePages() {
        return freePagesQueue.size();
    }

    @Override
    public int usedPages() {
        return numberPages() - freePages();
    }

    @Override
    public long fileSize() {
        return this.fileSize;
    }

    @Override
    public Page initPage(int pageNo) {
        Page p = new Page();
        p.pageNo = pageNo;
        p.empty = true;
        p.dirty = false;
        p.data = new byte[PageSize.getCurrent()];
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/initPage, page=" + p);
        return p;
    }

    @Override
    public boolean ensurePage(int pageNo) throws Exception {
        return pageNo > totalNumberOfPages - 1;  // Extend will take place on the next access (auto extension)
    }

    @Override
    public Page createPage() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/createPage ...");
        if (!freePagesQueue.isEmpty()) {
            int pageNo = freePagesQueue.poll();
            return initPage(pageNo);
        }
        Page page = loadPage(totalNumberOfPages);
        freePagesQueue.remove(page.pageNo); // A new region was added and that page is now part of the free pages
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/createPage, page=" + page + " done");
        return page;
    }

    public Page loadPage(int pageNo) throws IOException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/loadPage, pageNo=" + pageNo);
        MappedFileRegion region = findRegionForPage(pageNo);
        if (region == null) {
            throw new IOException("Page number " + pageNo + " does not exist in any region");
        }
        return region.getPage(pageNo);
    }

    @Override
    public void freePage(Page page) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/freePage, pageNo=" + page.pageNo);
        MappedFileRegion region = findRegionForPage(page.pageNo);
        if (region == null) {
            throw new IOException("Page number " + page.pageNo + " does not exist in any region");
        }
        region.freePage(page.pageNo);
        if (freePoolEnabled)
            freePagesQueue.add(page.pageNo);
    }

    public void writePage(Page page) throws IOException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/writePage, pageNo=" + page.pageNo);
        MappedFileRegion region = findRegionForPage(page.pageNo);
        if (region == null) {
            throw new IOException("Page number " + page.pageNo + " does not exist in any region");
        }
        region.writePage(page);
    }

    @Override
    public void shrink() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/shrink ...");
        boolean shrinked = false;
        for (int i = regions.size() - 1; i > 0; i--) { // Iterate from last to second region (first region always remains)
            MappedFileRegion region = regions.get(i);
            if (region.isEmpty()) {
                region.close(); // Close the region
                regions.remove(i); // Remove the region from the list
                shrinked = true;
            } else {
                break; // Stop if a non-empty region is found
            }
        }
        if (shrinked) {
            adjustFileSize(regions.getFirst().getRegionStartPage() * pageSize);
            freePagesQueue.clear();
            totalNumberOfPages = regions.size() * targetRegionSizeInPages;
            scanAllRegionsForFreePages();
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/shrink done");
    }

    public void sync() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/sync");
        regions.parallelStream().forEach(MappedFileRegion::sync);
    }

    @Override
    public void reset() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/reset ...");
        close();
        init();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/reset done");
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/close ...");
        regions.forEach(MappedFileRegion::close);
        regions.clear();
        freePagesQueue.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/close done");
    }

    @Override
    public void deleteStore() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/deleteStore ,,,");
        close();
        new File(filePath).delete();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/deleteStore done");
    }

    public String toString() {
        return "MappedFileRegionController, fileSize=" + fileSize + ", freePoolEnabled=" + freePoolEnabled + ", regions=" + regions.size() + ", totalNumberOfPages=" + totalNumberOfPages + ", freePages=" + freePagesQueue.size();
    }
}
