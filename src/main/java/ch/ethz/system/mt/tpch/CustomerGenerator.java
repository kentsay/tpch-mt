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
package ch.ethz.system.mt.tpch;

import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static ch.ethz.system.mt.tpch.GenerateUtils.calculateRowCount;
import static ch.ethz.system.mt.tpch.GenerateUtils.calculateStartIndex;
import static java.util.Locale.ENGLISH;

public class CustomerGenerator
        implements TpchSchemaInterface<Customer>
{
    public static final int SCALE_BASE = 150_000;
    private static final int ACCOUNT_BALANCE_MIN = -99999;
    private static final int ACCOUNT_BALANCE_MAX = 999999;
    private static final int ADDRESS_AVERAGE_LENGTH = 25;
    private static final int COMMENT_AVERAGE_LENGTH = 73;

    private final double scaleFactor;
    private final int part;
    private final int partCount;

    public int tenantSize = 0;
    public int dataPerTenant = 0;
    public int lastTenantData = 0;

    private final Distributions distributions;
    private final TextPool textPool;

    public CustomerGenerator(double scaleFactor, int part, int partCount, int tenantSize)
    {
        this(scaleFactor, part, partCount, tenantSize, Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    public CustomerGenerator(double scaleFactor, int part, int partCount, int tenantSize, Distributions distributions, TextPool textPool)
    {
        checkArgument(scaleFactor > 0, "scaleFactor must be greater than 0");
        checkArgument(part >= 1, "part must be at least 1");
        checkArgument(part <= partCount, "part must be less than or equal to part count");
        checkArgument(tenantSize > 0, "tenant number must be greater than 0");

        this.scaleFactor = scaleFactor;
        this.part = part;
        this.partCount = partCount;
        this.tenantSize = tenantSize;
        this.dataPerTenant  = (int) calculateRowCount(SCALE_BASE, scaleFactor, part, partCount)/tenantSize;
        this.lastTenantData = this.dataPerTenant + (int) calculateRowCount(SCALE_BASE, scaleFactor, part, partCount) % tenantSize;

        this.distributions = checkNotNull(distributions, "distributions is null");
        this.textPool = checkNotNull(textPool, "textPool is null");
    }

    @Override
    public Iterator<Customer> iterator()
    {

        return new CustomerGeneratorIterator(
                distributions,
                textPool,
                new int[] {tenantSize, dataPerTenant, lastTenantData},
                calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
                calculateRowCount(SCALE_BASE, scaleFactor, part, partCount));
    }

    private static class CustomerGeneratorIterator
            extends AbstractIterator<Customer>
    {
        private final RandomAlphaNumeric addressRandom = new RandomAlphaNumeric(881155353, ADDRESS_AVERAGE_LENGTH);
        private final RandomBoundedInt nationKeyRandom;
        private final RandomPhoneNumber phoneRandom = new RandomPhoneNumber(1521138112);
        private final RandomBoundedInt accountBalanceRandom = new RandomBoundedInt(298370230, ACCOUNT_BALANCE_MIN, ACCOUNT_BALANCE_MAX);
        private final RandomString marketSegmentRandom;
        private final RandomText commentRandom;

        private final long startIndex;
        private final long rowCount;

        private long index;
        private long counter = 0;
        private int[] dataBlock;

        private CustomerGeneratorIterator(Distributions distributions, TextPool textPool, int[] dataBlock, long startIndex, long rowCount)
        {
            this.startIndex = startIndex;
            this.rowCount = rowCount;
            this.dataBlock = dataBlock;

            nationKeyRandom = new RandomBoundedInt(1489529863, 0, distributions.getNations().size() - 1);
            marketSegmentRandom = new RandomString(1140279430, distributions.getMarketSegments());
            commentRandom = new RandomText(1335826707, textPool, COMMENT_AVERAGE_LENGTH);

            addressRandom.advanceRows(startIndex);
            nationKeyRandom.advanceRows(startIndex);
            phoneRandom.advanceRows(startIndex);
            accountBalanceRandom.advanceRows(startIndex);
            marketSegmentRandom.advanceRows(startIndex);
            commentRandom.advanceRows(startIndex);
        }

        @Override
        protected Customer computeNext()
        {
            if (index >= rowCount) {
                return endOfData();
            }

            if (index <= (dataBlock[0]-1)*dataBlock[1]) {
                if ((startIndex + counter + 1) > dataBlock[1]) {
                    counter = 0;
                }
            }

            Customer customer = makeCustomer(startIndex + counter + 1);

            addressRandom.rowFinished();
            nationKeyRandom.rowFinished();
            phoneRandom.rowFinished();
            accountBalanceRandom.rowFinished();
            marketSegmentRandom.rowFinished();
            commentRandom.rowFinished();

            counter++;
            index++;

            return customer;
        }

        private Customer makeCustomer(long customerKey)
        {
            long nationKey = nationKeyRandom.nextValue();

            return new Customer(customerKey,
                    customerKey,
                    String.format(ENGLISH, "Customer#%09d", customerKey),
                    addressRandom.nextValue(),
                    nationKey,
                    phoneRandom.nextValue(nationKey),
                    accountBalanceRandom.nextValue(),
                    marketSegmentRandom.nextValue(),
                    commentRandom.nextValue());
        }
    }
}
