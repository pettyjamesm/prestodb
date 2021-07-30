/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.exchange;

import com.facebook.presto.common.Page;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.exchange.PageReference.PageReleasedListener;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

class PartitioningExchanger
        implements LocalExchanger
{
    private final List<Consumer<PageReference>> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final PartitionFunction partitionFunction;
    private final int[] partitioningChannels;
    private final int hashChannel; // when < 0, no hash channel is present
    private final IntArrayList[] partitionAssignments;
    private final PageReleasedListener onPageReleased;

    public PartitioningExchanger(
            List<Consumer<PageReference>> partitions,
            LocalExchangeMemoryManager memoryManager,
            PartitionFunction partitionFunction,
            List<Integer> partitioningChannels,
            Optional<Integer> hashChannel)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.partitioningChannels = Ints.toArray(requireNonNull(partitioningChannels, "partitioningChannels is null"));
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null").orElse(-1);
        this.onPageReleased = PageReleasedListener.forLocalExchangeMemoryManager(memoryManager);

        partitionAssignments = new IntArrayList[partitions.size()];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
        }
    }

    @Override
    public void accept(Page page)
    {
        // Ensure that the whole page is loaded and LazyBlocks unwrapped before acquiring the lock
        Page inputPage = page.getLoadedPage();
        partition(inputPage, extractPartitioningChannels(inputPage));
    }

    private synchronized void partition(Page inputPage, Page partitioningChannelsPage)
    {
        // assign each row to a partition, assignments lists are expected to have been reset and ready
        for (int position = 0; position < partitioningChannelsPage.getPositionCount(); position++) {
            int partition = partitionFunction.getPartition(partitioningChannelsPage, position);
            partitionAssignments[partition].add(position);
        }

        // build a page for each partition
        for (int partition = 0; partition < partitionAssignments.length; partition++) {
            IntArrayList positions = partitionAssignments[partition];
            int partitionPositions = positions.size();
            if (partitionPositions > 0) {
                Page pageSplit;
                if (partitionPositions == inputPage.getPositionCount()) {
                    pageSplit = inputPage; // all rows assigned to this partition, no copy necessary
                }
                else {
                    pageSplit = inputPage.copyPositions(positions.elements(), 0, partitionPositions);
                }
                // reset the assignments list for future use
                positions.clear();
                memoryManager.updateMemoryUsage(pageSplit.getRetainedSizeInBytes());
                buffers.get(partition).accept(new PageReference(pageSplit, 1, onPageReleased));
            }
        }
    }

    private Page extractPartitioningChannels(Page inputPage)
    {
        // hash value is pre-computed, only needs to extract that channel
        if (hashChannel >= 0) {
            return inputPage.extractChannel(hashChannel);
        }

        // extract partitioning channels
        return inputPage.extractChannels(partitioningChannels);
    }

    @Override
    public ListenableFuture<?> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
