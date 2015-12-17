using NBitcoin.DataEncoders;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.OpenAsset
{
    public class ColorMarker : IBitcoinSerializable
    {
        // Open Asset Tag
        public const ushort OATag = 0x414f;
        // For Lykke protocol we choose LK
        public const ushort LKTag = 0x4b4c;
        // Private variable holding the Tag property value;
        private static ushort _tag = OATag;
        public static ushort Tag
        {
            get
            {
                return _tag;
            }
            set
            {
                _tag = value;
            }

        }
        public static ColorMarker TryParse(string script)
        {
            return TryParse(new Script(script));
        }

        public static ColorMarker TryParse(Transaction transaction)
        {
            foreach (var script in transaction.Outputs.Select(o => o.ScriptPubKey))
            {
                ColorMarker marker = TryParse(script);
                if (marker != null)
                    return marker;
            }
            return null;
        }
        public static ColorMarker TryParse(Script script)
        {
            try
            {
                ColorMarker result = new ColorMarker();
                if (!result.ReadScript(script))
                    return null;
                return result;
            }
            catch (EndOfStreamException)
            {
                return null;
            }
        }

        private bool ReadScript(Script script)
        {
            var bytes = script.ToBytes(true);
            if (bytes.Length == 0 || bytes[0] != (byte)OpcodeType.OP_RETURN)
                return false;
            foreach (var op in script.ToOps())
            {
                if (op.PushData != null && !op.IsInvalid)
                {
                    if (ReadData(op.PushData))
                        return true;
                }
            }
            return false;
        }

        private bool ReadData(byte[] data)
        {
            try
            {
                BitcoinStream stream = new BitcoinStream(data);
                ushort marker = 0;
                stream.ReadWrite(ref marker);
                if (marker != Tag)
                    return false;
                stream.ReadWrite(ref _Version);
                if (_Version != 1 && _Version != 2)
                    return false;

                if (_Version == 2)
                {
                    stream.ReadWrite(ref _OpCode);
                }

                ulong quantityCount = 0;
                stream.ReadWriteAsVarInt(ref quantityCount);
                Quantities = new ulong[quantityCount];

                for (ulong i = 0; i < quantityCount; i++)
                {
                    Quantities[i] = ReadLEB128(stream);
                    if (Quantities[i] > MAX_QUANTITY)
                        return false;
                }

                stream.ReadWriteAsVarString(ref _Metadata);
                if (stream.Inner.Position != data.Length)
                    return false;

                if (Version == 2)
                {
                    switch (OpCode)
                    {
                        case 0x01:
                            var metadatalength = Quantities.Count();
                            ExchangeFlags = new bool[metadatalength];
                            var metadataByteCount = Math.Ceiling(metadatalength / 8.0);
                            for (int i = 0; i < metadataByteCount; i++)
                            {
                                for (int j = 0; j < 8; j++)
                                {
                                    if (i * 8 + j >= ExchangeFlags.Count())
                                    {
                                        break;
                                    }

                                    ExchangeFlags[metadatalength - 1 - (i * 8 + j)] = ((Metadata[i] & (byte)Math.Pow(2, j)) == 1);
                                }
                            }
                            break;
                    }
                }

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private static ulong ReadLEB128(BitcoinStream stream)
        {
            ulong value = 0;
            value = stream.ReadWrite((byte)0);
            if ((value & 128uL) == 0uL)
            {
                return value;
            }
            value &= 127uL;
            ulong chunk = stream.ReadWrite((byte)0);
            value |= (chunk & 127uL) << 7;
            if ((chunk & 128uL) == 0uL)
            {
                return value;
            }
            chunk = stream.ReadWrite((byte)0);
            value |= (chunk & 127uL) << 14;
            if ((chunk & 128uL) == 0uL)
            {
                return value;
            }
            chunk = stream.ReadWrite((byte)0);
            value |= (chunk & 127uL) << 21;
            if ((chunk & 128uL) == 0uL)
            {
                return value;
            }
            chunk = stream.ReadWrite((byte)0);
            value |= (chunk & 127uL) << 28;
            if ((chunk & 128uL) == 0uL)
            {
                return value;
            }
            chunk = stream.ReadWrite((byte)0);
            value |= (chunk & 127uL) << 35;
            if ((chunk & 128uL) == 0uL)
            {
                return value;
            }
            chunk = stream.ReadWrite((byte)0);
            value |= (chunk & 127uL) << 42;
            if ((chunk & 128uL) == 0uL)
            {
                return value;
            }
            chunk = stream.ReadWrite((byte)0);
            value |= (chunk & 127uL) << 49;
            if ((chunk & 128uL) == 0uL)
            {
                return value;
            }
            chunk = stream.ReadWrite((byte)0);
            value |= (chunk & 127uL) << 56;
            if ((chunk & 128uL) == 0uL)
            {
                return value;
            }
            chunk = stream.ReadWrite((byte)0);
            value |= chunk << 63;
            if ((chunk & 18446744073709551614uL) != 0uL)
            {
                throw new FormatException("Invalid LEB128 number");
            }
            return value;
        }
        private void WriteLEB128(ulong value, BitcoinStream stream)
        {
            byte[] bytes = new byte[10];
            int ioIndex = 0;
            int count = 0;
            do
            {
                bytes[ioIndex++] = (byte)((value & 127uL) | 128uL);
                count++;
            }
            while ((value >>= 7) != 0uL);
            Array.Resize(ref bytes, count);
            bytes[bytes.Length - 1] &= 127;
            stream.ReadWrite(ref bytes);
        }

        public ColorMarker()
        {
            Quantities = new ulong[0];
        }
        public ColorMarker(Script script)
        {
            if (!ReadScript(script))
                throw new FormatException("Not a color marker");
        }

        public ColorMarker(ulong[] quantities)
        {
            if (quantities == null)
                throw new ArgumentNullException("quantities");
            Quantities = quantities;
        }
        ushort _Version = 1;
        public ushort Version
        {
            get
            {
                return _Version;
            }
            set
            {
                _Version = value;
            }
        }

        // Added for version 2
        // 0x01 means Send Asset to exchange
        // 0x02 means exhcnage operation
        byte _OpCode = 0x01;
        public byte OpCode
        {
            get
            {
                return _OpCode;
            }
            set
            {
                _OpCode = value;
            }
        }

        ulong[] _Quantities;
        public ulong[] Quantities
        {
            get
            {
                return _Quantities;
            }
            set
            {
                _Quantities = value;
            }
        }

        bool[] _ExchangeFlags;

        public bool[] ExchangeFlags
        {
            get
            {
                return _ExchangeFlags;
            }
            set
            {
                _ExchangeFlags = value;
            }
        }

        public void SetQuantity(uint index, long quantity)
        {
            if (Quantities == null)
                Quantities = new ulong[0];
            if (Quantities.Length <= index)
                Array.Resize(ref _Quantities, (int)index + 1);
            Quantities[index] = checked((ulong)quantity);
        }

        public void SetQuantity(int index, long quantity)
        {
            SetQuantity((uint)index, quantity);
        }

        public void SetExchangeFlag(int index)
        {
            if (ExchangeFlags == null)
            {
                ExchangeFlags = new bool[0];
            }
            if (ExchangeFlags.Length <= index)
                Array.Resize(ref _ExchangeFlags, (int)index + 1);
            ExchangeFlags[index] = true;
        }

        byte[] _Metadata = new byte[0];
        public byte[] Metadata
        {
            get
            {
                return _Metadata;
            }
            set
            {
                _Metadata = value;
            }
        }
        private const ulong MAX_QUANTITY = ((1UL << 63) - 1);

        public Script GetScript()
        {
            var bytes = ToBytes();
            return _Template.GenerateScriptPubKey(bytes);
        }

        public byte[] ToBytes()
        {
            MemoryStream ms = new MemoryStream();
            BitcoinStream stream = new BitcoinStream(ms, true);
            stream.ReadWrite(Tag);
            stream.ReadWrite(ref _Version);
            switch (Version)
            {
                case 2:
                    stream.ReadWrite(ref _OpCode);
                    break;
                default:
                    break;
            }
            var quantityCount = (uint)this.Quantities.Length;
            stream.ReadWriteAsVarInt(ref quantityCount);
            for (int i = 0; i < quantityCount; i++)
            {
                if (Quantities[i] > MAX_QUANTITY)
                    throw new ArgumentOutOfRangeException("Quantity should not exceed " + Quantities[i]);
                WriteLEB128(Quantities[i], stream);
            }

            switch (Version)
            {
                case 1:
                    break;
                case 2:
                    switch (OpCode)
                    {
                        case 0x01: // Transfer to exchange
                            uint metadataLength = (uint)Math.Ceiling(ExchangeFlags.Count() / 8.0);
                            int exchangeFlagCount = ExchangeFlags.Count();
                            if (metadataLength > 0)
                            {
                                Array.Resize(ref _Metadata, (int)metadataLength);
                                for (int i = 0; i < metadataLength; i++)
                                {
                                    _Metadata[i] = 0;
                                    for (int j = 0; j < 8; j++)
                                    {
                                        if (i * 8 + j >= ExchangeFlags.Count())
                                        {
                                            break;
                                        }
                                        _Metadata[i] += (byte)((ExchangeFlags[exchangeFlagCount - 1 - (i * 8 + j)] ? (byte)1 : (byte)0) * (byte)Math.Pow(2, j));
                                    }
                                }
                            }
                            break;
                        case 0x02: // Exchange operation
                            break;
                    }
                    break;
            }

            stream.ReadWriteAsVarString(ref _Metadata);
            return ms.ToArray();
        }
        static readonly TxNullDataTemplate _Template = new TxNullDataTemplate(1024 * 5);

        public static ColorMarker Get(Transaction transaction)
        {
            uint i = 0;
            return Get(transaction, out i);
        }

        public static ColorMarker Get(Transaction transaction, out uint markerPosition)
        {
            uint resultIndex = 0;
            var result = transaction.Outputs.Select(o => TryParse(o.ScriptPubKey)).Where((o, i) =>
            {
                resultIndex = (uint)i;
                return o != null;
            }).FirstOrDefault();
            markerPosition = resultIndex;
            return result;
        }

        #region IBitcoinSerializable Members

        public void ReadWrite(BitcoinStream stream)
        {
            if (stream.Serializing)
            {
                var script = GetScript();
                stream.ReadWrite(ref script);
            }
            else
            {
                Script script = null;
                stream.ReadWrite(ref script);
                if (!ReadScript(script))
                {
                    throw new FormatException("Invalid ColorMarker");
                }
            }
        }

        #endregion

        public static bool HasValidColorMarker(Transaction tx)
        {
            var marker = Get(tx);
            if (marker == null)
                return false;
            //If there are more items in the  asset quantity list  than the number of colorable outputs, the transaction is deemed invalid, and all outputs are uncolored.
            return marker.HasValidQuantitiesCount(tx);
        }

        public bool HasValidQuantitiesCount(Transaction tx)
        {
            return Quantities.Length <= tx.Outputs.Count - 1;
        }

        public Uri GetMetadataUrl()
        {
            if (Metadata == null || Metadata.Length == 0)
                return null;
            var result = Encoders.ASCII.EncodeData(Metadata);
            if (!result.StartsWith("u="))
                return null;
            Uri uri = null;
            Uri.TryCreate(result.Substring(2), UriKind.Absolute, out uri);
            return uri;
        }

        public void SetMetadataUrl(Uri uri)
        {
            if (uri == null)
            {
                Metadata = new byte[0];
                return;
            }
            Metadata = Encoders.ASCII.DecodeData("u=" + uri.AbsoluteUri);
            return;
        }

        // Added to support exchange operation
        public void SetMetadata(byte[] value)
        {
            Array.Resize(ref _Metadata, 20);
            Array.Copy(value, Metadata, value.Length);
        }
    }
}
