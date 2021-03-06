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

import static ch.ethz.system.mt.tpch.PartGenerator.calculatePartPrice;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Locale.ENGLISH;

public class OrderGenerator
        implements TpchSchemaInterface<Order>
{
    public static final int SCALE_BASE = 1_500_000;

    // portion with have no orders
    public static final int CUSTOMER_MORTALITY = 3;

    private static final int ORDER_DATE_MIN = GenerateUtils.MIN_GENERATE_DATE;
    private static final int ORDER_DATE_MAX = ORDER_DATE_MIN + (GenerateUtils.TOTAL_DATE_RANGE - LineItemGenerator.ITEM_SHIP_DAYS - 1);
    private static final int CLERK_SCALE_BASE = 1000;

    private static final int LINE_COUNT_MIN = 1;
    static final int LINE_COUNT_MAX = 7;

    private static final int COMMENT_AVERAGE_LENGTH = 49;

    private static final int ORDER_KEY_SPARSE_BITS = 2;
    private static final int ORDER_KEY_SPARSE_KEEP = 3;

    private final double scaleFactor;
    private final int part;
    private final int partCount;

    private final Distributions distributions;
    private final TextPool textPool;

    public int[] distDataSize;
    public int[] custDataSize;

    public OrderGenerator(double scaleFactor, int part, int partCount, int[] distBlockSize)
    {
        this(scaleFactor, part, partCount, distBlockSize, Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    public OrderGenerator(double scaleFactor, int part, int partCount, int[] distBlockSize, int[] custDataSize)
    {
        this(scaleFactor, part, partCount, distBlockSize, custDataSize, Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    public OrderGenerator(double scaleFactor, int part, int partCount, int[] distBlockSize, Distributions distributions, TextPool textPool)
    {
        checkArgument(scaleFactor > 0, "scaleFactor must be greater than 0");
        checkArgument(part >= 1, "part must be at least 1");
        checkArgument(part <= partCount, "part must be less than or equal to part count");

        this.scaleFactor = scaleFactor;
        this.part = part;
        this.partCount = partCount;
        this.distDataSize = distBlockSize;
        this.distributions = checkNotNull(distributions, "distributions is null");
        this.textPool = checkNotNull(textPool, "textPool is null");
    }

    public OrderGenerator(double scaleFactor, int part, int partCount, int[] distBlockSize, int[] custDataSize, Distributions distributions, TextPool textPool)
    {
        checkArgument(scaleFactor > 0, "scaleFactor must be greater than 0");
        checkArgument(part >= 1, "part must be at least 1");
        checkArgument(part <= partCount, "part must be less than or equal to part count");

        this.scaleFactor = scaleFactor;
        this.part = part;
        this.partCount = partCount;
        this.distDataSize = distBlockSize;
        this.custDataSize = custDataSize;
        this.distributions = checkNotNull(distributions, "distributions is null");
        this.textPool = checkNotNull(textPool, "textPool is null");
    }

    @Override
    public Iterator<Order> iterator()
    {
        return new OrderGeneratorIterator(
                distributions,
                textPool,
                scaleFactor,
                distDataSize,
                custDataSize,
                GenerateUtils.calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
                GenerateUtils.calculateRowCount(SCALE_BASE, scaleFactor, part, partCount));
    }

    private static class OrderGeneratorIterator
            extends AbstractIterator<Order>
    {
        private final RandomBoundedInt orderDateRandom = createOrderDateRandom();
        private final RandomBoundedInt lineCountRandom = createLineCountRandom();
        private RandomBoundedLong customerKeyRandom;
        private final RandomString orderPriorityRandom;
        private final RandomBoundedInt clerkRandom;
        private final RandomText commentRandom;

        private final RandomBoundedInt lineQuantityRandom = LineItemGenerator.createQuantityRandom();
        private final RandomBoundedInt lineDiscountRandom = LineItemGenerator.createDiscountRandom();
        private final RandomBoundedInt lineTaxRandom = LineItemGenerator.createTaxRandom();
        private final RandomBoundedLong linePartKeyRandom;
        private final RandomBoundedInt lineShipDateRandom = LineItemGenerator.createShipDateRandom();

        private final long startIndex;
        private final long rowCount;

        private long maxCustomerKey;

        private long index;
        private long counter = 0;
        private int[] dataBlock;
        private int[] custDataSize;
        private int dataSizeIndex = 0;
        double scaleFactor;

        private OrderGeneratorIterator(Distributions distributions, TextPool textPool, double scaleFactor, int[] dataBlock, int[] custSize, long startIndex, long rowCount)
        {
            this.startIndex = startIndex;
            this.rowCount = rowCount;
            this.dataBlock = dataBlock;
            this.custDataSize = custSize;
            this.scaleFactor = scaleFactor;

            clerkRandom = new RandomBoundedInt(1171034773, 1, Math.max((int) (scaleFactor * CLERK_SCALE_BASE), CLERK_SCALE_BASE));

            //change the maxCustomerKey by dividing with number of tenants to avoid data not found issue
            //maxCustomerKey = (long) ((CustomerGenerator.SCALE_BASE * scaleFactor) / dataBlock.length);
            //customerKeyRandom = new RandomBoundedLong(851767375, scaleFactor >= 30000, 1, maxCustomerKey);

            orderPriorityRandom = new RandomString(591449447, distributions.getOrderPriorities());
            commentRandom = new RandomText(276090261, textPool, COMMENT_AVERAGE_LENGTH);

            linePartKeyRandom = LineItemGenerator.createPartKeyRandom(scaleFactor);

            orderDateRandom.advanceRows(startIndex);
            lineCountRandom.advanceRows(startIndex);
            //customerKeyRandom.advanceRows(startIndex);
            orderPriorityRandom.advanceRows(startIndex);
            clerkRandom.advanceRows(startIndex);
            commentRandom.advanceRows(startIndex);

            lineQuantityRandom.advanceRows(startIndex);
            lineDiscountRandom.advanceRows(startIndex);
            lineShipDateRandom.advanceRows(startIndex);
            lineTaxRandom.advanceRows(startIndex);
            linePartKeyRandom.advanceRows(startIndex);
        }

        @Override
        protected Order computeNext()
        {
            if (index >= rowCount) {
                return endOfData();
            }

            maxCustomerKey = custDataSize[dataSizeIndex];
            customerKeyRandom = new RandomBoundedLong(851767375, scaleFactor >= 30000, 1, maxCustomerKey);
            customerKeyRandom.advanceRows(startIndex);

            //if the counter reach the size of that tenant, move to the next index and get the size of that tenant
            if ((startIndex + counter + 1) > dataBlock[dataSizeIndex]) {
                dataSizeIndex++;
                counter = 0;
            }

            Order order = makeOrder(startIndex + counter + 1);

            orderDateRandom.rowFinished();
            lineCountRandom.rowFinished();
            customerKeyRandom.rowFinished();
            orderPriorityRandom.rowFinished();
            clerkRandom.rowFinished();
            commentRandom.rowFinished();

            lineQuantityRandom.rowFinished();
            lineDiscountRandom.rowFinished();
            lineShipDateRandom.rowFinished();
            lineTaxRandom.rowFinished();
            linePartKeyRandom.rowFinished();

            counter++;
            index++;

            return order;
        }

        private Order makeOrder(long index)
        {
            long orderKey = makeOrderKey(index);

            int orderDate = orderDateRandom.nextValue();

            // generate customer key, taking into account customer mortality rate
            long customerKey = customerKeyRandom.nextValue();
            int delta = 1;
            while (customerKey % CUSTOMER_MORTALITY == 0) {
                customerKey += delta;
                customerKey = Math.min(customerKey, maxCustomerKey);
                delta *= -1;
            }


            long totalPrice = 0;
            int shippedCount = 0;

            int lineCount = lineCountRandom.nextValue();
            for (long lineNumber = 0; lineNumber < lineCount; lineNumber++) {
                int quantity = lineQuantityRandom.nextValue();
                int discount = lineDiscountRandom.nextValue();
                int tax = lineTaxRandom.nextValue();

                long partKey = linePartKeyRandom.nextValue();

                long partPrice = calculatePartPrice(partKey);
                long extendedPrice = partPrice * quantity;
                long discountedPrice = extendedPrice * (100 - discount);
                totalPrice += ((discountedPrice / 100) * (100 + tax)) / 100;

                int shipDate = lineShipDateRandom.nextValue();
                shipDate += orderDate;
                if (GenerateUtils.isInPast(shipDate)) {
                    shippedCount++;
                }
            }

            char orderStatus;
            if (shippedCount == lineCount) {
                orderStatus = 'F';
            }
            else if (shippedCount > 0) {
                orderStatus = 'P';
            }
            else {
                orderStatus = 'O';
            }

            return new Order(
                    orderKey,
                    orderKey,
                    customerKey,
                    orderStatus,
                    totalPrice,
                    GenerateUtils.toEpochDate(orderDate),
                    orderPriorityRandom.nextValue(),
                    String.format(ENGLISH, "Clerk#%09d", clerkRandom.nextValue()),
                    0,
                    commentRandom.nextValue());
        }
    }

    static RandomBoundedInt createLineCountRandom()
    {
        return new RandomBoundedInt(1434868289, LINE_COUNT_MIN, LINE_COUNT_MAX);
    }

    static RandomBoundedInt createOrderDateRandom()
    {
        return new RandomBoundedInt(1066728069, ORDER_DATE_MIN, ORDER_DATE_MAX);
    }

    static long makeOrderKey(long orderIndex)
    {
        long low_bits = orderIndex & ((1 << ORDER_KEY_SPARSE_KEEP) - 1);

        long ok = orderIndex;
        ok = ok >> ORDER_KEY_SPARSE_KEEP;
        ok = ok << ORDER_KEY_SPARSE_BITS;
        ok = ok << ORDER_KEY_SPARSE_KEEP;
        ok += low_bits;

        return ok;
    }
}
