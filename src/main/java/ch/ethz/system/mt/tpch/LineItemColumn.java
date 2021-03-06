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

import static ch.ethz.system.mt.tpch.GenerateUtils.formatDate;

public enum LineItemColumn
        implements TpchColumn<LineItem>
{
    @SuppressWarnings("SpellCheckingInspection")
    ORDER_KEY("orderkey", TpchColumnType.BIGINT)
            {
                public long getLong(LineItem lineItem)
                {
                    return lineItem.getOrderKey();
                }
            },

//    remove the partKey column in tpch-mt version
//    @SuppressWarnings("SpellCheckingInspection")
//    PART_KEY("partkey", TpchColumnType.BIGINT)
//            {
//                public long getLong(LineItem lineItem)
//                {
//                    return lineItem.getPartKey();
//                }
//            },

    @SuppressWarnings("SpellCheckingInspection")
    SUPPLIER_KEY("suppkey", TpchColumnType.BIGINT)
            {
                public long getLong(LineItem lineItem)
                {
                    return lineItem.getSupplierKey();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    LINE_NUMBER("linenumber", TpchColumnType.BIGINT)
            {
                public long getLong(LineItem lineItem)
                {
                    return lineItem.getLineNumber();
                }
            },

    QUANTITY("quantity", TpchColumnType.BIGINT)
            {
                public long getLong(LineItem lineItem)
                {
                    return lineItem.getQuantity();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    EXTENDED_PRICE("extendedprice", TpchColumnType.DOUBLE)
            {
                public double getDouble(LineItem lineItem)
                {
                    return lineItem.getExtendedPrice();
                }

                public long getLong(LineItem lingItem)
                {
                    return lingItem.getExtendedPriceInCents();
                }
            },

    DISCOUNT("discount", TpchColumnType.DOUBLE)
            {
                public double getDouble(LineItem lineItem)
                {
                    return lineItem.getDiscount();
                }

                public long getLong(LineItem lineItem)
                {
                    return lineItem.getDiscountPercent();
                }
            },

    TAX("tax", TpchColumnType.DOUBLE)
            {
                public double getDouble(LineItem lineItem)
                {
                    return lineItem.getTax();
                }

                public long getLong(LineItem lineItem)
                {
                    return lineItem.getTaxPercent();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    RETURN_FLAG("returnflag", TpchColumnType.VARCHAR)
            {
                public String getString(LineItem lineItem)
                {
                    return lineItem.getReturnFlag();
                }
            },

    STATUS("linestatus", TpchColumnType.VARCHAR)
            {
                public String getString(LineItem lineItem)
                {
                    return lineItem.getStatus();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_DATE("shipdate", TpchColumnType.DATE)
            {
                public String getString(LineItem lineItem)
                {
                    return formatDate(getDate(lineItem));
                }

                public int getDate(LineItem lineItem)
                {
                    return lineItem.getShipDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    COMMIT_DATE("commitdate", TpchColumnType.DATE)
            {
                public String getString(LineItem lineItem)
                {
                    return formatDate(getDate(lineItem));
                }

                public int getDate(LineItem lineItem)
                {
                    return lineItem.getCommitDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    RECEIPT_DATE("receiptdate", TpchColumnType.DATE)
            {
                public String getString(LineItem lineItem)
                {
                    return formatDate(getDate(lineItem));
                }

                @Override
                public int getDate(LineItem lineItem)
                {
                    return lineItem.getReceiptDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_INSTRUCTIONS("shipinstruct", TpchColumnType.VARCHAR)
            {
                public String getString(LineItem lineItem)
                {
                    return lineItem.getShipInstructions();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_MODE("shipmode", TpchColumnType.VARCHAR)
            {
                public String getString(LineItem lineItem)
                {
                    return lineItem.getShipMode();
                }
            },

    COMMENT("comment", TpchColumnType.VARCHAR)
            {
                public String getString(LineItem lineItem)
                {
                    return lineItem.getComment();
                }
            };


    private final String columnName;
    private final TpchColumnType type;

    LineItemColumn(String columnName, TpchColumnType type)
    {
        this.columnName = columnName;
        this.type = type;
    }

    @Override
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public TpchColumnType getType()
    {
        return type;
    }

    @Override
    public double getDouble(LineItem lineItem)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(LineItem lineItem)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(LineItem lineItem)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDate(LineItem entity)
    {
        throw new UnsupportedOperationException();
    }
}
