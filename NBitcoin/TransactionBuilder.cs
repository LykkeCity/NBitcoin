﻿using NBitcoin.DataEncoders;
using NBitcoin.OpenAsset;
using NBitcoin.Policy;
using NBitcoin.Stealth;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Builder = System.Func<NBitcoin.TransactionBuilder.TransactionBuildingContext, NBitcoin.IMoney>;

namespace NBitcoin
{
    [Flags]
    public enum ChangeType : int
    {
        All = 3,
        Colored = 1,
        Uncolored = 2
    }
    public interface ICoinSelector
    {
        IEnumerable<ICoin> Select(IEnumerable<ICoin> coins, IMoney target);
    }

    /// <summary>
    /// Algorithm implemented by bitcoin core https://github.com/bitcoin/bitcoin/blob/master/src/wallet.cpp#L1276
    /// Minimize the change
    /// </summary>
    public class DefaultCoinSelector : ICoinSelector
    {
        public DefaultCoinSelector()
        {

        }
        Random _Rand = new Random();
        public DefaultCoinSelector(int seed)
        {
            _Rand = new Random(seed);
        }
        #region ICoinSelector Members

        public IEnumerable<ICoin> Select(IEnumerable<ICoin> coins, IMoney target)
        {
            var zero = target.Sub(target);
            var targetCoin = coins
                            .FirstOrDefault(c => c.Amount.CompareTo(target) == 0);
            //If any of your UTXO² matches the Target¹ it will be used.
            if (targetCoin != null)
                return new[] { targetCoin };

            List<ICoin> result = new List<ICoin>();
            IMoney total = zero;

            if (target.CompareTo(zero) == 0)
                return result;

            var orderedCoins = coins.OrderBy(s => s.Amount).ToArray();

            foreach (var coin in orderedCoins)
            {
                if (coin.Amount.CompareTo(target) == -1 && total.CompareTo(target) == -1)
                {
                    total = total.Add(coin.Amount);
                    result.Add(coin);
                    //If the "sum of all your UTXO smaller than the Target" happens to match the Target, they will be used. (This is the case if you sweep a complete wallet.)
                    if (total.CompareTo(target) == 0)
                        return result;

                }
                else
                {
                    if (total.CompareTo(target) == -1 && coin.Amount.CompareTo(target) == 1)
                    {
                        //If the "sum of all your UTXO smaller than the Target" doesn't surpass the target, the smallest UTXO greater than your Target will be used.
                        return new[] { coin };
                    }
                    else
                    {
                        //						Else Bitcoin Core does 1000 rounds of randomly combining unspent transaction outputs until their sum is greater than or equal to the Target. If it happens to find an exact match, it stops early and uses that.
                        //Otherwise it finally settles for the minimum of
                        //the smallest UTXO greater than the Target
                        //the smallest combination of UTXO it discovered in Step 4.
                        var allCoins = orderedCoins.ToArray();
                        IMoney minTotal = null;
                        List<ICoin> minSelection = null;
                        for (int _ = 0; _ < 1000; _++)
                        {
                            var selection = new List<ICoin>();
                            Shuffle(allCoins, _Rand);
                            total = zero;
                            for (int i = 0; i < allCoins.Length; i++)
                            {
                                selection.Add(allCoins[i]);
                                total = total.Add(allCoins[i].Amount);
                                if (total.CompareTo(target) == 0)
                                    return selection;
                                if (total.CompareTo(target) == 1)
                                    break;
                            }
                            if (total.CompareTo(target) == -1)
                            {
                                return null;
                            }
                            if (minTotal == null || total.CompareTo(minTotal) == -1)
                            {
                                minTotal = total;
                                minSelection = selection;
                            }
                        }
                    }
                }
            }
            if (total.CompareTo(target) == -1)
                return null;
            return result;
        }

        internal static void Shuffle<T>(T[] list, Random random)
        {
            int n = list.Length;
            while (n > 1)
            {
                n--;
                int k = random.Next(n + 1);
                T value = list[k];
                list[k] = list[n];
                list[n] = value;
            }
        }
        internal static void Shuffle<T>(List<T> list, Random random)
        {
            int n = list.Count;
            while (n > 1)
            {
                n--;
                int k = random.Next(n + 1);
                T value = list[k];
                list[k] = list[n];
                list[n] = value;
            }
        }


        #endregion
    }

    /// <summary>
    /// Exception thrown when not enough funds are present for verifying or building a transaction
    /// </summary>
    public class NotEnoughFundsException : Exception
    {
        public NotEnoughFundsException()
        {
        }
        public NotEnoughFundsException(string message, string group, IMoney missing)
            : base(BuildMessage(message, group, missing))
        {
            Missing = missing;
            Group = group;
        }

        private static string BuildMessage(string message, string group, IMoney missing)
        {
            StringBuilder builder = new StringBuilder();
            builder.Append(message);
            if (group != null)
                builder.Append(" in group " + group);
            if (missing != null)
                builder.Append(" with missing amount " + missing);
            return builder.ToString();
        }
        public NotEnoughFundsException(string message, Exception inner)
            : base(message, inner)
        {
        }

        /// <summary>
        /// The group name who is missing the funds
        /// </summary>
        public string Group
        {
            get;
            set;
        }

        /// <summary>
        /// Amount of Money missing
        /// </summary>
        public IMoney Missing
        {
            get;
            set;
        }
    }

    /// <summary>
    /// A class for building and signing all sort of transactions easily (http://www.codeproject.com/Articles/835098/NBitcoin-Build-Them-All)
    /// </summary>
    public class TransactionBuilder
    {
        internal class TransactionSigningContext
        {
            public TransactionSigningContext(TransactionBuilder builder, Transaction transaction)
            {
                Builder = builder;
                Transaction = transaction;
            }

            public Transaction Transaction
            {
                get;
                set;
            }
            public TransactionBuilder Builder
            {
                get;
                set;
            }

            private readonly List<Key> _AdditionalKeys = new List<Key>();
            public List<Key> AdditionalKeys
            {
                get
                {
                    return _AdditionalKeys;
                }
            }

            public SigHash SigHash
            {
                get;
                set;
            }
        }
        internal class TransactionBuildingContext
        {
            public TransactionBuildingContext(TransactionBuilder builder)
            {
                Builder = builder;
                Transaction = new Transaction();
                AdditionalFees = Money.Zero;
            }
            public TransactionBuilder.BuilderGroup Group
            {
                get;
                set;
            }

            private readonly List<ICoin> _ConsumedCoins = new List<ICoin>();
            public List<ICoin> ConsumedCoins
            {
                get
                {
                    return _ConsumedCoins;
                }
            }
            public TransactionBuilder Builder
            {
                get;
                set;
            }
            public Transaction Transaction
            {
                get;
                set;
            }

            public Money AdditionalFees
            {
                get;
                set;
            }

            private readonly List<Builder> _AdditionalBuilders = new List<Builder>();
            public List<Builder> AdditionalBuilders
            {
                get
                {
                    return _AdditionalBuilders;
                }
            }

            ColorMarker _Marker;

            public ColorMarker GetColorMarker(bool issuance)
            {
                if (_Marker == null)
                    _Marker = new ColorMarker();
                if (!issuance)
                    EnsureMarkerInserted();
                return _Marker;
            }

            private TxOut EnsureMarkerInserted()
            {
                uint position;
                if (ColorMarker.Get(Transaction, out position) != null)
                    return Transaction.Outputs[position];
                var txout = Transaction.AddOutput(new TxOut()
                {
                    ScriptPubKey = new ColorMarker().GetScript()
                });
                txout.Value = Money.Zero;
                return txout;
            }

            public void Finish()
            {
                if (_Marker != null)
                {
                    var txout = EnsureMarkerInserted();
                    txout.ScriptPubKey = _Marker.GetScript();
                }
            }

            public IssuanceCoin IssuanceCoin
            {
                get;
                set;
            }

            public IMoney ChangeAmount
            {
                get;
                set;
            }

            public TransactionBuildingContext CreateMemento()
            {
                var memento = new TransactionBuildingContext(Builder);
                memento.RestoreMemento(this);
                return memento;
            }

            public void RestoreMemento(TransactionBuildingContext memento)
            {
                _Marker = memento._Marker == null ? null : new ColorMarker(memento._Marker.GetScript());
                Transaction = memento.Transaction.Clone();
                AdditionalFees = memento.AdditionalFees;
            }

            public bool NonFinalSequenceSet
            {
                get;
                set;
            }

            public IMoney CoverOnly
            {
                get;
                set;
            }

            public IMoney Dust
            {
                get;
                set;
            }

            public ChangeType ChangeType
            {
                get;
                set;
            }
        }

        internal class BuilderGroup
        {
            TransactionBuilder _Parent;
            public BuilderGroup(TransactionBuilder parent)
            {
                _Parent = parent;
                Builders.Add(SetChange);
            }

            IMoney SetChange(TransactionBuildingContext ctx)
            {
                var changeAmount = (Money)ctx.ChangeAmount;
                if (changeAmount.Satoshi == 0)
                    return Money.Zero;
                ctx.Transaction.AddOutput(new TxOut(changeAmount, ctx.Group.ChangeScript[(int)ChangeType.Uncolored]));
                return changeAmount;
            }
            internal List<Builder> Builders = new List<Builder>();
            internal Dictionary<OutPoint, ICoin> Coins = new Dictionary<OutPoint, ICoin>();
            internal List<Builder> IssuanceBuilders = new List<Builder>();
            internal Dictionary<AssetId, List<Builder>> BuildersByAsset = new Dictionary<AssetId, List<Builder>>();
            internal Script[] ChangeScript = new Script[3];
            internal void Shuffle()
            {
                Shuffle(Builders);
                foreach (var builders in BuildersByAsset)
                    Shuffle(builders.Value);
                Shuffle(IssuanceBuilders);
            }
            private void Shuffle(List<Builder> builders)
            {
                DefaultCoinSelector.Shuffle(builders, _Parent._Rand);
            }

            public Money CoverOnly
            {
                get;
                set;
            }

            public string Name
            {
                get;
                set;
            }
        }

        List<BuilderGroup> _BuilderGroups = new List<BuilderGroup>();
        BuilderGroup _CurrentGroup = null;
        internal BuilderGroup CurrentGroup
        {
            get
            {
                if (_CurrentGroup == null)
                {
                    _CurrentGroup = new BuilderGroup(this);
                    _BuilderGroups.Add(_CurrentGroup);
                }
                return _CurrentGroup;
            }
        }
        public TransactionBuilder(string metadataRepoAssemblyName = "NBitcoin, Version=2.1.5.2, Culture=neutral, PublicKeyToken=null",
                    string metadataRepoTypeName = "NBitcoin.OpenAsset.SimpleMetadataColoredCoinTransactionRepository")
        {
            _Rand = new Random();
            CoinSelector = new DefaultCoinSelector();
            StandardTransactionPolicy = new StandardTransactionPolicy();
            DustPrevention = true;
            _MetadataRepository = (IMetadataColoredCoinTransactionRepository)Activator.
                CreateInstance(metadataRepoAssemblyName, metadataRepoTypeName).Unwrap();
        }
        internal Random _Rand;

        internal IMetadataColoredCoinTransactionRepository _MetadataRepository;
        public TransactionBuilder(int seed)
        {
            _Rand = new Random(seed);
            CoinSelector = new DefaultCoinSelector(seed);
            StandardTransactionPolicy = new StandardTransactionPolicy();
            DustPrevention = true;
        }

        public ICoinSelector CoinSelector
        {
            get;
            set;
        }

        /// <summary>
        /// Will transform transfers below Dust, so the transaction get correctly relayed by the network.
        /// </summary>
        public bool DustPrevention
        {
            get;
            set;
        }

        /// <summary>
        /// A callback used by the TransactionBuilder when it does not find the coin for an input
        /// </summary>
        public Func<OutPoint, ICoin> CoinFinder
        {
            get;
            set;
        }

        /// <summary>
        /// A callback used by the TransactionBuilder when it does not find the key for a scriptPubKey
        /// </summary>
        public Func<Script, Key> KeyFinder
        {
            get;
            set;
        }

        LockTime? _LockTime;
        public TransactionBuilder SetLockTime(LockTime lockTime)
        {
            _LockTime = lockTime;
            return this;
        }

        List<Key> _Keys = new List<Key>();

        public TransactionBuilder AddKeys(params ISecret[] keys)
        {
            _Keys.AddRange(keys.Select(k => k.PrivateKey));
            return this;
        }

        public TransactionBuilder AddKeys(params Key[] keys)
        {
            _Keys.AddRange(keys);
            return this;
        }

        public TransactionBuilder AddCoins(params ICoin[] coins)
        {
            return AddCoins((IEnumerable<ICoin>)coins);
        }

        public TransactionBuilder AddCoins(IEnumerable<ICoin> coins)
        {
            foreach (var coin in coins)
            {
                CurrentGroup.Coins.AddOrReplace(coin.Outpoint, coin);
            }
            return this;
        }

        /// <summary>
        /// Set the name of this group (group are separated by call to Then())
        /// </summary>
        /// <param name="groupName">Name of the group</param>
        /// <returns></returns>
        public TransactionBuilder SetGroupName(string groupName)
        {
            CurrentGroup.Name = groupName;
            return this;
        }

        /// <summary>
        /// Send bitcoins to a destination
        /// </summary>
        /// <param name="destination">The destination</param>
        /// <param name="amount">The amount</param>
        /// <returns></returns>
        public TransactionBuilder Send(IDestination destination, Money amount)
        {
            return Send(destination.ScriptPubKey, amount);
        }

        readonly static TxNullDataTemplate _OpReturnTemplate = new TxNullDataTemplate(1024 * 1024);

        /// <summary>
        /// Send bitcoins to a destination
        /// </summary>
        /// <param name="scriptPubKey">The destination</param>
        /// <param name="amount">The amount</param>
        /// <returns></returns>
        public TransactionBuilder Send(Script scriptPubKey, Money amount)
        {
            if (amount < Money.Zero)
                throw new ArgumentOutOfRangeException("amount", "amount can't be negative");
            if (DustPrevention && amount < GetDust(scriptPubKey) && !_OpReturnTemplate.CheckScriptPubKey(scriptPubKey))
            {
                SendFees(amount);
                return this;
            }
            CurrentGroup.Builders.Add(ctx =>
            {
                ctx.Transaction.Outputs.Add(new TxOut(amount, scriptPubKey));
                return amount;
            });
            return this;
        }

        /// <summary>
        /// Send a money amount to the destination
        /// </summary>
        /// <param name="destination">The destination</param>
        /// <param name="amount">The amount (supported : Money, AssetMoney, MoneyBag)</param>
        /// <returns></returns>
        /// <exception cref="System.NotSupportedException">The coin type is not supported</exception>
        public TransactionBuilder Send(IDestination destination, IMoney amount)
        {
            return Send(destination.ScriptPubKey, amount);
        }
        /// <summary>
        /// Send a money amount to the destination
        /// </summary>
        /// <param name="destination">The destination</param>
        /// <param name="amount">The amount (supported : Money, AssetMoney, MoneyBag)</param>
        /// <returns></returns>
        /// <exception cref="System.NotSupportedException">The coin type is not supported</exception>
        public TransactionBuilder Send(Script scriptPubKey, IMoney amount)
        {
            MoneyBag bag = amount as MoneyBag;
            if (bag != null)
            {
                foreach (var money in bag)
                    Send(scriptPubKey, amount);
                return this;
            }
            Money coinAmount = amount as Money;
            if (coinAmount != null)
                return Send(scriptPubKey, coinAmount);
            AssetMoney assetAmount = amount as AssetMoney;
            if (assetAmount != null)
                return SendAsset(scriptPubKey, assetAmount);
            throw new NotSupportedException("Type of Money not supported");
        }

        /// <summary>
        /// Send assets (Open Asset) to a destination
        /// </summary>
        /// <param name="destination">The destination</param>
        /// <param name="asset">The asset and amount</param>
        /// <returns></returns>
        public TransactionBuilder SendAsset(IDestination destination, AssetMoney asset)
        {
            return SendAsset(destination.ScriptPubKey, asset);
        }

        public TransactionBuilder SendAssetToExchange(IDestination destination, AssetMoney asset)
        {
            return SendAssetToExchange(destination.ScriptPubKey, asset);
        }

        /// <summary>
        /// Send assets (Open Asset) to a destination
        /// </summary>
        /// <param name="destination">The destination</param>
        /// <param name="asset">The asset and amount</param>
        /// <returns></returns>
        public TransactionBuilder SendAsset(IDestination destination, AssetId assetId, ulong quantity)
        {
            return SendAsset(destination, new AssetMoney(assetId, quantity));
        }

        public TransactionBuilder Shuffle()
        {
            DefaultCoinSelector.Shuffle(_BuilderGroups, _Rand);
            foreach (var group in _BuilderGroups)
                group.Shuffle();
            return this;
        }

        IMoney SetColoredChange(TransactionBuildingContext ctx)
        {
            var changeAmount = (AssetMoney)ctx.ChangeAmount;
            if (changeAmount.Quantity == 0)
                return changeAmount;
            var marker = ctx.GetColorMarker(false);
            var script = ctx.Group.ChangeScript[(int)ChangeType.Colored];
            var txout = ctx.Transaction.AddOutput(new TxOut(GetDust(script), script));
            marker.SetQuantity(ctx.Transaction.Outputs.Count - 2, changeAmount.Quantity);
            ctx.AdditionalFees += txout.Value;
            return changeAmount;
        }

        public TransactionBuilder SendAsset(Script scriptPubKey, AssetId assetId, ulong assetQuantity)
        {
            return SendAsset(scriptPubKey, new AssetMoney(assetId, assetQuantity));
        }

        public TransactionBuilder SendAsset(Script scriptPubKey, AssetMoney asset)
        {
            if (asset.Quantity < 0)
                throw new ArgumentOutOfRangeException("asset", "Asset amount can't be negative");
            AssertOpReturn("Colored Coin");
            var builders = CurrentGroup.BuildersByAsset.TryGet(asset.Id);
            if (builders == null)
            {
                builders = new List<Builder>();
                CurrentGroup.BuildersByAsset.Add(asset.Id, builders);
                builders.Add(SetColoredChange);
            }
            builders.Add(ctx =>
            {
                var marker = ctx.GetColorMarker(false);
                var txout = ctx.Transaction.AddOutput(new TxOut(GetDust(scriptPubKey), scriptPubKey));
                marker.SetQuantity(ctx.Transaction.Outputs.Count - 2, asset.Quantity);
                ctx.AdditionalFees += txout.Value;
                return asset;
            });
            return this;
        }

        public TransactionBuilder SendAssetToExchange(Script scriptPubKey, AssetMoney asset)
        {
            if (asset.Quantity < 0)
                throw new ArgumentOutOfRangeException("asset", "Asset amount can't be negative");
            AssertOpReturn("Colored Coin");
            var builders = CurrentGroup.BuildersByAsset.TryGet(asset.Id);
            if (builders == null)
            {
                builders = new List<Builder>();
                CurrentGroup.BuildersByAsset.Add(asset.Id, builders);
                builders.Add(SetColoredChange);
            }
            builders.Add(ctx =>
            {
                var marker = ctx.GetColorMarker(false);

                if (marker.OpCode != 0x01)
                {
                    // OpCode has been set previously
                    throw new InvalidOperationException("OpCode of the marker has been previously been set to something other than exchange transfer");
                }

                if (marker.Version < 2)
                {
                    marker.Version = 2;
                }
                marker.OpCode = 0x01;
                var txout = ctx.Transaction.AddOutput(new TxOut(GetDust(scriptPubKey), scriptPubKey));
                marker.SetQuantity(ctx.Transaction.Outputs.Count - 2, asset.Quantity);
                marker.SetExchangeFlag(ctx.Transaction.Outputs.Count - 2);
                ctx.AdditionalFees += txout.Value;
                return asset;
            });
            return this;
        }

        public TransactionBuilder PerformExchangeOperation(IDestination destination01, AssetMoney asset01,
            IDestination destination02, AssetMoney asset02, string reason)
        {
            return PerformExchangeOperation(destination01.ScriptPubKey, asset01, destination02.ScriptPubKey,
                asset02, reason);
        }

        /// <summary>
        /// Perform the required steps, specifically about marker output to post an exchange
        /// in the output of the transaction, the transaction will have exactly
        /// 4 colored coin outputs, two for each of the inputs
        /// Of the 2 outputs one go to the exchange as return and the other one to the destination
        /// </summary>
        /// <param name="scriptPubKey01">The scriptPubKey for the first customer</param>
        /// <param name="asset01">The asset going to the first customer</param>
        /// <param name="scriptPubKey02">The scriptPubKey for the second customer</param>
        /// <param name="asset02">The asset going to the second customer</param>
        /// <param name="reason">The reason why the exchange operation is valid, it is json
        /// string containing the signed requests from the customers for the exchange operation.</param>
        /// <returns>The transaction builder object to be used further</returns>
        public TransactionBuilder PerformExchangeOperation(Script scriptPubKey01, AssetMoney asset01,
            Script scriptPubKey02, AssetMoney asset02, string reason)
        {
            if (asset01.Quantity < 0 || asset02.Quantity < 0)
                throw new ArgumentOutOfRangeException("asset", "Asset amount can't be negative");
            AssertOpReturn("Colored Coin");
            if (CurrentGroup.BuildersByAsset.Count != 0)
            {
                throw new InvalidOperationException
                    ("The exchange operation should be the only operation for colored coins in the transaction.");
            }

            // First asset
            var builders01 = new List<Builder>();
            CurrentGroup.BuildersByAsset.Add(asset01.Id, builders01);
            builders01.Add(SetColoredChange);
            builders01.Add(ctx =>
            {
                var marker = ctx.GetColorMarker(false);
                if (marker.Version < 2)
                {
                    marker.Version = 2;
                }
                marker.OpCode = 0x02;
                var txout = ctx.Transaction.AddOutput(new TxOut(GetDust(scriptPubKey01), scriptPubKey01));
                marker.SetQuantity(ctx.Transaction.Outputs.Count - 2, asset01.Quantity);
                marker.SetExchangeFlag(ctx.Transaction.Outputs.Count - 2);
                ctx.AdditionalFees += txout.Value;

                // Adding the SHA1 hash of reason 
                // Idea taken from Colu implementation of colored coin
                // https://github.com/Colored-Coins/Colored-Coins-Protocol-Specification/wiki/Metadata
                SHA1 hashingFunction = SHA1.Create();
                byte[] hash = hashingFunction.ComputeHash(System.Text.Encoding.ASCII.GetBytes(reason));
                marker.SetMetadata(hash);
                _MetadataRepository.AddMetadataToRepository(reason);
                return asset01;
            });

            var builders02 = new List<Builder>();
            CurrentGroup.BuildersByAsset.Add(asset02.Id, builders02);
            builders02.Add(SetColoredChange);
            builders02.Add(ctx =>
            {
                var marker = ctx.GetColorMarker(false);
                var txout = ctx.Transaction.AddOutput(new TxOut(GetDust(scriptPubKey02), scriptPubKey02));
                marker.SetQuantity(ctx.Transaction.Outputs.Count - 2, asset02.Quantity);
                marker.SetExchangeFlag(ctx.Transaction.Outputs.Count - 2);
                ctx.AdditionalFees += txout.Value;
                return asset02;
            });

            return this;
        }


        Money GetDust()
        {
            return GetDust(new Script(new byte[25]));
        }
        Money GetDust(Script script)
        {
            if (StandardTransactionPolicy == null || StandardTransactionPolicy.MinRelayTxFee == null)
                return Money.Zero;
            return new TxOut(Money.Zero, script).GetDustThreshold(StandardTransactionPolicy.MinRelayTxFee);
        }

        /// <summary>
        /// Set transaction policy fluently
        /// </summary>
        /// <param name="policy">The policy</param>
        /// <returns>this</returns>
        public TransactionBuilder SetTransactionPolicy(StandardTransactionPolicy policy)
        {
            StandardTransactionPolicy = policy;
            return this;
        }
        public StandardTransactionPolicy StandardTransactionPolicy
        {
            get;
            set;
        }


        string _OpReturnUser;
        private void AssertOpReturn(string name)
        {
            if (_OpReturnUser == null)
            {
                _OpReturnUser = name;
            }
            else
            {
                if (_OpReturnUser != name)
                    throw new InvalidOperationException("Op return already used for " + _OpReturnUser);
            }
        }

        public TransactionBuilder Send(BitcoinStealthAddress address, Money amount, Key ephemKey = null)
        {
            if (amount < Money.Zero)
                throw new ArgumentOutOfRangeException("amount", "amount can't be negative");

            if (_OpReturnUser == null)
                _OpReturnUser = "Stealth Payment";
            else
                throw new InvalidOperationException("Op return already used for " + _OpReturnUser);

            CurrentGroup.Builders.Add(ctx =>
            {
                var payment = address.CreatePayment(ephemKey);
                payment.AddToTransaction(ctx.Transaction, amount);
                return amount;
            });
            return this;
        }

        public TransactionBuilder IssueAsset(IDestination destination, AssetMoney asset)
        {
            return IssueAsset(destination.ScriptPubKey, asset);
        }

        AssetId _IssuedAsset;

        public TransactionBuilder IssueAsset(Script scriptPubKey, AssetMoney asset)
        {
            AssertOpReturn("Colored Coin");
            if (_IssuedAsset == null)
                _IssuedAsset = asset.Id;
            else if (_IssuedAsset != asset.Id)
                throw new InvalidOperationException("You can issue only one asset type in a transaction");

            CurrentGroup.IssuanceBuilders.Add(ctx =>
            {
                var marker = ctx.GetColorMarker(true);
                if (ctx.IssuanceCoin == null)
                {
                    var issuance = ctx.Group.Coins.Values.OfType<IssuanceCoin>().Where(i => i.AssetId == asset.Id).FirstOrDefault();
                    if (issuance == null)
                        throw new InvalidOperationException("No issuance coin for emitting asset found");
                    ctx.IssuanceCoin = issuance;
                    ctx.Transaction.Inputs.Insert(0, new TxIn(issuance.Outpoint));
                    ctx.AdditionalFees -= issuance.Bearer.Amount;
                    if (issuance.DefinitionUrl != null)
                    {
                        marker.SetMetadataUrl(issuance.DefinitionUrl);
                    }
                }

                ctx.Transaction.Outputs.Insert(0, new TxOut(GetDust(scriptPubKey), scriptPubKey));
                marker.Quantities = new[] { checked((ulong)asset.Quantity) }.Concat(marker.Quantities).ToArray();
                ctx.AdditionalFees += ctx.Transaction.Outputs[0].Value;
                return asset;
            });
            return this;
        }

        public TransactionBuilder SendFees(Money fees)
        {
            if (fees == null)
                throw new ArgumentNullException("fees");
            CurrentGroup.Builders.Add(ctx => fees);
            return this;
        }

        /// <summary>
        /// Split the estimated fees accross the several groups (separated by Then())
        /// </summary>
        /// <param name="feeRate"></param>
        /// <returns></returns>
        public TransactionBuilder SendEstimatedFees(FeeRate feeRate)
        {
            var tx = BuildTransaction(false);
            var fees = EstimateFees(tx, feeRate);
            SendFees(fees);
            return this;
        }

        /// <summary>
        /// Estimate the fee needed for the transaction, and split among groups
        /// </summary>
        /// <param name="feeRate"></param>
        /// <returns></returns>
        public TransactionBuilder SendEstimatedFeesSplit(FeeRate feeRate)
        {
            var tx = BuildTransaction(false);
            var fees = EstimateFees(tx, feeRate);
            return SendFeesSplit(fees);
        }
        /// <summary>
        /// Split the fees accross the several groups (separated by Then())
        /// </summary>
        /// <param name="fees"></param>
        /// <returns></returns>
        public TransactionBuilder SendFeesSplit(Money fees)
        {
            if (fees == null)
                throw new ArgumentNullException("fees");
            var perGroup = fees.Split(_BuilderGroups.Count).GetEnumerator();
            foreach (var group in _BuilderGroups)
            {
                perGroup.MoveNext();
                var fee = perGroup.Current;
                group.Builders.Add(ctx => fee);
            }
            return this;
        }

        public TransactionBuilder SetChange(IDestination destination, ChangeType changeType = ChangeType.All)
        {
            return SetChange(destination.ScriptPubKey, changeType);
        }

        public TransactionBuilder SetChange(Script scriptPubKey, ChangeType changeType = ChangeType.All)
        {
            if ((changeType & ChangeType.Colored) != 0)
            {
                CurrentGroup.ChangeScript[(int)ChangeType.Colored] = scriptPubKey;
            }
            if ((changeType & ChangeType.Uncolored) != 0)
            {
                CurrentGroup.ChangeScript[(int)ChangeType.Uncolored] = scriptPubKey;
            }
            return this;
        }

        public TransactionBuilder SetCoinSelector(ICoinSelector selector)
        {
            if (selector == null)
                throw new ArgumentNullException("selector");
            CoinSelector = selector;
            return this;
        }
        /// <summary>
        /// Build the transaction
        /// </summary>
        /// <param name="sign">True if signs all inputs with the available keys</param>
        /// <returns>The transaction</returns>
        /// <exception cref="NBitcoin.NotEnoughFundsException">Not enough funds are available</exception>
        public Transaction BuildTransaction(bool sign)
        {
            return BuildTransaction(sign, SigHash.All);
        }

        /// <summary>
        /// Build the transaction
        /// </summary>
        /// <param name="sign">True if signs all inputs with the available keys</param>
        /// <param name="sigHash">The type of signature</param>
        /// <returns>The transaction</returns>
        /// <exception cref="NBitcoin.NotEnoughFundsException">Not enough funds are available</exception>
        public Transaction BuildTransaction(bool sign, SigHash sigHash)
        {
            TransactionBuildingContext ctx = new TransactionBuildingContext(this);
            if (_CompletedTransaction != null)
                ctx.Transaction = _CompletedTransaction;
            if (_LockTime != null)
                ctx.Transaction.LockTime = _LockTime.Value;
            foreach (var group in _BuilderGroups)
            {
                ctx.Group = group;
                ctx.AdditionalBuilders.Clear();
                ctx.AdditionalFees = Money.Zero;

                ctx.ChangeType = ChangeType.Colored;
                foreach (var builder in group.IssuanceBuilders)
                    builder(ctx);

                var buildersByAsset = group.BuildersByAsset.ToList();
                foreach (var builders in buildersByAsset)
                {
                    var coins = group.Coins.Values.OfType<ColoredCoin>().Where(c => c.Amount.Id == builders.Key);

                    ctx.Dust = new AssetMoney(builders.Key);
                    ctx.CoverOnly = null;
                    ctx.ChangeAmount = new AssetMoney(builders.Key);
                    var btcSpent = BuildTransaction(ctx, group, builders.Value, coins, new AssetMoney(builders.Key))
                        .OfType<IColoredCoin>().Select(c => c.Bearer.Amount).Sum();
                    ctx.AdditionalFees -= btcSpent;
                }

                ctx.AdditionalBuilders.Add(_ => _.AdditionalFees);
                ctx.Dust = GetDust();
                ctx.ChangeAmount = Money.Zero;
                ctx.CoverOnly = group.CoverOnly;
                ctx.ChangeType = ChangeType.Uncolored;
                BuildTransaction(ctx, group, group.Builders, group.Coins.Values.OfType<Coin>(), Money.Zero);
            }
            ctx.Finish();

            if (sign)
            {
                SignTransactionInPlace(ctx.Transaction, sigHash);
            }
            return ctx.Transaction;
        }

        private IEnumerable<ICoin> BuildTransaction(
            TransactionBuildingContext ctx,
            BuilderGroup group,
            IEnumerable<Builder> builders,
            IEnumerable<ICoin> coins,
            IMoney zero)
        {
            var originalCtx = ctx.CreateMemento();
            var target = builders.Concat(ctx.AdditionalBuilders).Select(b => b(ctx)).Sum(zero);
            if (ctx.CoverOnly != null)
            {
                target = ctx.CoverOnly.Add(ctx.ChangeAmount);
            }

            var unconsumed = coins.Where(c => ctx.ConsumedCoins.All(cc => cc.Outpoint != c.Outpoint));
            var selection = CoinSelector.Select(unconsumed, target);
            if (selection == null)
                throw new NotEnoughFundsException("Not enough fund to cover the target",
                    group.Name,
                    target.Sub(unconsumed.Select(u => u.Amount).Sum(zero))
                    );
            var total = selection.Select(s => s.Amount).Sum(zero);
            var change = total.Sub(target);
            if (change.CompareTo(zero) == -1)
                throw new NotEnoughFundsException("Not enough fund to cover the target",
                    group.Name,
                    change.Negate()
                );
            if (change.CompareTo(ctx.Dust) == 1)
            {
                var changeScript = group.ChangeScript[(int)ctx.ChangeType];
                if (changeScript == null)
                    throw new InvalidOperationException("A change address should be specified (" + ctx.ChangeType + ")");
                if (!(ctx.Dust is Money) || change.CompareTo(GetDust(changeScript)) == 1)
                {
                    ctx.RestoreMemento(originalCtx);
                    ctx.ChangeAmount = change;
                    try
                    {
                        return BuildTransaction(ctx, group, builders, coins, zero);
                    }
                    finally
                    {
                        ctx.ChangeAmount = zero;
                    }
                }
            }
            foreach (var coin in selection)
            {
                ctx.ConsumedCoins.Add(coin);
                var input = ctx.Transaction.Inputs.FirstOrDefault(i => i.PrevOut == coin.Outpoint);
                if (input == null)
                    input = ctx.Transaction.AddInput(new TxIn(coin.Outpoint));
                if (_LockTime != null && !ctx.NonFinalSequenceSet)
                {
                    input.Sequence = 0;
                    ctx.NonFinalSequenceSet = true;
                }
            }
            return selection;
        }

        public Transaction SignTransaction(Transaction transaction, SigHash sigHash)
        {
            var tx = transaction.Clone();
            SignTransactionInPlace(tx, sigHash);
            return tx;
        }

        public Transaction SignTransaction(Transaction transaction)
        {
            return SignTransaction(transaction, SigHash.All);
        }
        public Transaction SignTransactionInPlace(Transaction transaction)
        {
            return SignTransactionInPlace(transaction, SigHash.All);
        }
        public Transaction SignTransactionInPlace(Transaction transaction, SigHash sigHash)
        {
            TransactionSigningContext ctx = new TransactionSigningContext(this, transaction);
            ctx.SigHash = sigHash;
            foreach (var input in transaction.Inputs.AsIndexedInputs())
            {
                var coin = FindSignableCoin(input.TxIn);
                if (coin != null)
                {
                    Sign(ctx, coin, input);
                }
            }
            return transaction;
        }

        public ICoin FindSignableCoin(TxIn txIn)
        {
            var coin = FindCoin(txIn.PrevOut);
            if (coin == null)
                return coin;
            if (coin is IColoredCoin)
                coin = ((IColoredCoin)coin).Bearer;

            if (PayToScriptHashTemplate.Instance.CheckScriptPubKey(coin.TxOut.ScriptPubKey))
            {
                var scriptCoin = coin as IScriptCoin;
                if (scriptCoin == null)
                {
                    var expectedId = PayToScriptHashTemplate.Instance.ExtractScriptPubKeyParameters(coin.TxOut.ScriptPubKey);
                    //Try to extract redeem from this transaction
                    var p2shParams = PayToScriptHashTemplate.Instance.ExtractScriptSigParameters(txIn.ScriptSig, coin.TxOut.ScriptPubKey);
                    if (p2shParams == null || p2shParams.RedeemScript.Hash != expectedId)
                    {
                        var redeem = _ScriptIdToRedeem.TryGet(expectedId);
                        if (redeem == null)
                            return null;
                        //throw new InvalidOperationException("A coin with a P2SH scriptPubKey was detected, however this coin is not a ScriptCoin, and no information about the redeem script was found in the input, and from the KnownRedeems");
                        else
                            return ((Coin)coin).ToScriptCoin(redeem);
                    }
                    else
                    {
                        return ((Coin)coin).ToScriptCoin(p2shParams.RedeemScript);
                    }
                }
            }
            return coin;
        }

        /// <summary>
        /// Verify that a transaction is fully signed and have enough fees
        /// </summary>
        /// <param name="tx">The transaction to check</param>
        /// <returns>True if no error</returns>
        public bool Verify(Transaction tx)
        {
            TransactionPolicyError[] errors;
            return Verify(tx, null as Money, out errors);
        }
        /// <summary>
        /// Verify that a transaction is fully signed and have enough fees
        /// </summary>
        /// <param name="tx">The transaction to check</param>
        /// <param name="expectedFees">The expected fees (more or less 10%)</param>
        /// <returns>True if no error</returns>
        public bool Verify(Transaction tx, Money expectedFees)
        {
            TransactionPolicyError[] errors;
            return Verify(tx, expectedFees, out errors);
        }

        /// <summary>
        /// Verify that a transaction is fully signed and have enough fees
        /// </summary>
        /// <param name="tx">The transaction to check</param>
        /// <param name="expectedFeeRate">The expected fee rate</param>
        /// <returns>True if no error</returns>
        public bool Verify(Transaction tx, FeeRate expectedFeeRate)
        {
            TransactionPolicyError[] errors;
            return Verify(tx, expectedFeeRate, out errors);
        }

        /// <summary>
        /// Verify that a transaction is fully signed and have enough fees
        /// </summary>
        /// <param name="tx">The transaction to check</param>
        /// <param name="errors">Detected errors</param>
        /// <returns>True if no error</returns>
        public bool Verify(Transaction tx, out TransactionPolicyError[] errors)
        {
            return Verify(tx, null as Money, out errors);
        }
        /// <summary>
        /// Verify that a transaction is fully signed, have enough fees, and follow the Standard and Miner Transaction Policy rules
        /// </summary>
        /// <param name="tx">The transaction to check</param>
        /// <param name="expectedFees">The expected fees (more or less 10%)</param>
        /// <param name="errors">Detected errors</param>
        /// <returns>True if no error</returns>
        public bool Verify(Transaction tx, Money expectedFees, out TransactionPolicyError[] errors)
        {
            if (tx == null)
                throw new ArgumentNullException("tx");
            var coins = tx.Inputs.Select(i => FindCoin(i.PrevOut)).Where(c => c != null).ToArray();
            List<TransactionPolicyError> exceptions = new List<TransactionPolicyError>();
            var policyErrors = MinerTransactionPolicy.Instance.Check(tx, coins);
            exceptions.AddRange(policyErrors);
            policyErrors = StandardTransactionPolicy.Check(tx, coins);
            exceptions.AddRange(policyErrors);
            if (expectedFees != null)
            {
                var fees = tx.GetFee(coins);
                if (fees != null)
                {
                    Money margin = Money.Zero;
                    if (DustPrevention)
                        margin = GetDust() * 2;
                    if (!fees.Almost(expectedFees, margin))
                        exceptions.Add(new NotEnoughFundsPolicyError("Fees different than expected", expectedFees - fees));
                }
            }
            errors = exceptions.ToArray();
            return errors.Length == 0;
        }
        /// <summary>
        /// Verify that a transaction is fully signed and have enough fees
        /// </summary>
        /// <param name="tx">The transaction to check</param>
        /// <param name="expectedFeeRate">The expected fee rate</param>
        /// <param name="errors">Detected errors</param>
        /// <returns>True if no error</returns>
        public bool Verify(Transaction tx, FeeRate expectedFeeRate, out TransactionPolicyError[] errors)
        {
            if (tx == null)
                throw new ArgumentNullException("tx");
            return Verify(tx, expectedFeeRate == null ? null : expectedFeeRate.GetFee(tx), out errors);
        }
        /// <summary>
        /// Verify that a transaction is fully signed and have enough fees
        /// </summary>
        /// <param name="tx">he transaction to check</param>
        /// <param name="expectedFeeRate">The expected fee rate</param>
        /// <returns>Detected errors</returns>
        public TransactionPolicyError[] Check(Transaction tx, FeeRate expectedFeeRate)
        {
            return Check(tx, expectedFeeRate == null ? null : expectedFeeRate.GetFee(tx));
        }
        /// <summary>
        /// Verify that a transaction is fully signed and have enough fees
        /// </summary>
        /// <param name="tx">he transaction to check</param>
        /// <param name="expectedFee">The expected fee</param>
        /// <returns>Detected errors</returns>
        public TransactionPolicyError[] Check(Transaction tx, Money expectedFee)
        {
            TransactionPolicyError[] errors;
            Verify(tx, expectedFee, out errors);
            return errors;
        }
        /// <summary>
        /// Verify that a transaction is fully signed and have enough fees
        /// </summary>
        /// <param name="tx">he transaction to check</param>
        /// <returns>Detected errors</returns>
        public TransactionPolicyError[] Check(Transaction tx)
        {
            return Check(tx, null as Money);
        }

        private CoinNotFoundException CoinNotFound(IndexedTxIn txIn)
        {
            return new CoinNotFoundException(txIn);
        }


        public ICoin FindCoin(OutPoint outPoint)
        {
            var result = _BuilderGroups.Select(c => c.Coins.TryGet(outPoint)).FirstOrDefault(r => r != null);
            if (result == null && CoinFinder != null)
                result = CoinFinder(outPoint);
            return result;
        }

        /// <summary>
        /// Find spent coins of a transaction
        /// </summary>
        /// <param name="tx">The transaction</param>
        /// <returns>Array of size tx.Input.Count, if a coin is not fund, a null coin is returned.</returns>
        public ICoin[] FindSpentCoins(Transaction tx)
        {
            return
                tx
                .Inputs
                .Select(i => FindCoin(i.PrevOut))
                .ToArray();
        }

        public int EstimateSize(Transaction tx)
        {
            if (tx == null)
                throw new ArgumentNullException("tx");
            var clone = tx.Clone();
            clone.Inputs.Clear();
            var baseSize = clone.ToBytes().Length;

            int inputSize = 0;
            foreach (var txin in tx.Inputs.AsIndexedInputs())
            {
                var coin = FindCoin(txin.PrevOut);
                if (coin == null)
                    throw CoinNotFound(txin);
                inputSize += EstimateScriptSigSize(coin) + 41;
            }

            return baseSize + inputSize;
        }

        static PubKey DummyPubKey = new PubKey(Encoders.Hex.DecodeData("022c2b9e61169fb1b1f2f3ff15ad52a21745e268d358ba821d36da7d7cd92dee0e"));
        static TransactionSignature DummySignature = new TransactionSignature(Encoders.Hex.DecodeData("3045022100b9d685584f46554977343009c04b3091e768c23884fa8d2ce2fb59e5290aa45302203b2d49201c7f695f434a597342eb32dfd81137014fcfb3bb5edc7a19c77774d201"));
        private int EstimateScriptSigSize(ICoin coin)
        {
            if (coin is IColoredCoin)
                coin = ((IColoredCoin)coin).Bearer;

            int size = 0;
            if (coin is ScriptCoin)
            {
                var scriptCoin = (ScriptCoin)coin;
                coin = new Coin(scriptCoin.Outpoint, new TxOut(scriptCoin.Amount, scriptCoin.Redeem));
                size += new Script(Op.GetPushOp(scriptCoin.Redeem.ToBytes(true))).Length;
            }

            var p2pk = PayToPubkeyTemplate.Instance.ExtractScriptPubKeyParameters(coin.TxOut.ScriptPubKey);
            if (p2pk != null)
            {
                size += PayToPubkeyTemplate.Instance.GenerateScriptSig(DummySignature).Length;
                return size;
            }

            var p2pkh = PayToPubkeyHashTemplate.Instance.ExtractScriptPubKeyParameters(coin.TxOut.ScriptPubKey);
            if (p2pkh != null)
            {
                size += PayToPubkeyHashTemplate.Instance.GenerateScriptSig(DummySignature, DummyPubKey).Length;
                return size;
            }

            var p2mk = PayToMultiSigTemplate.Instance.ExtractScriptPubKeyParameters(coin.TxOut.ScriptPubKey);
            if (p2mk != null)
            {
                size += PayToMultiSigTemplate.Instance.GenerateScriptSig(Enumerable.Range(0, p2mk.SignatureCount).Select(o => DummySignature).ToArray()).Length;
                return size;
            }

            size += coin.TxOut.ScriptPubKey.Length; //Using heurestic to approximate size of unknown scriptPubKey
            return size;
        }

        /// <summary>
        /// Estimate fees of an unsigned transaction
        /// </summary>
        /// <param name="tx"></param>
        /// <param name="feeRate">Fee rate</param>
        /// <returns></returns>
        public Money EstimateFees(Transaction tx, FeeRate feeRate)
        {
            if (tx == null)
                throw new ArgumentNullException("tx");
            if (feeRate == null)
                throw new ArgumentNullException("feeRate");

            var estimation = EstimateSize(tx);
            return feeRate.GetFee(estimation);
        }

        private void Sign(TransactionSigningContext ctx, ICoin coin, IndexedTxIn txIn)
        {
            var input = txIn.TxIn;
            if (coin is StealthCoin)
            {
                var stealthCoin = (StealthCoin)coin;
                var scanKey = FindKey(ctx, stealthCoin.Address.ScanPubKey.ScriptPubKey);
                if (scanKey == null)
                    throw new KeyNotFoundException("Scan key for decrypting StealthCoin not found");
                var spendKeys = stealthCoin.Address.SpendPubKeys.Select(p => FindKey(ctx, p.ScriptPubKey)).Where(p => p != null).ToArray();
                ctx.AdditionalKeys.AddRange(stealthCoin.Uncover(spendKeys, scanKey));
            }

            if (PayToScriptHashTemplate.Instance.CheckScriptPubKey(coin.TxOut.ScriptPubKey))
            {
                var scriptCoin = (IScriptCoin)coin;
                var original = input.ScriptSig;
                input.ScriptSig = CreateScriptSig(ctx, scriptCoin.Redeem, txIn);
                if (original != input.ScriptSig)
                {
                    input.ScriptSig = input.ScriptSig + Op.GetPushOp(scriptCoin.Redeem.ToBytes(true));
                }
            }
            else
            {
                input.ScriptSig = CreateScriptSig(ctx, coin.TxOut.ScriptPubKey, txIn);
            }

        }


        private Script CreateScriptSig(TransactionSigningContext ctx, Script scriptPubKey, IndexedTxIn txIn)
        {
            var originalScriptSig = txIn.TxIn.ScriptSig;
            txIn.TxIn.ScriptSig = scriptPubKey;

            var pubKeyHashParams = PayToPubkeyHashTemplate.Instance.ExtractScriptPubKeyParameters(scriptPubKey);
            if (pubKeyHashParams != null)
            {
                var key = FindKey(ctx, scriptPubKey);
                if (key == null)
                    return originalScriptSig;
                var sig = txIn.Sign(key, scriptPubKey, ctx.SigHash);
                return PayToPubkeyHashTemplate.Instance.GenerateScriptSig(sig, key.PubKey);
            }

            var multiSigParams = PayToMultiSigTemplate.Instance.ExtractScriptPubKeyParameters(scriptPubKey);
            if (multiSigParams != null)
            {
                var alreadySigned = PayToMultiSigTemplate.Instance.ExtractScriptSigParameters(originalScriptSig);
                if (alreadySigned == null && !Script.IsNullOrEmpty(originalScriptSig)) //Maybe a P2SH
                {
                    var ops = originalScriptSig.ToOps().ToList();
                    ops.RemoveAt(ops.Count - 1);
                    alreadySigned = PayToMultiSigTemplate.Instance.ExtractScriptSigParameters(new Script(ops));
                }
                List<TransactionSignature> signatures = new List<TransactionSignature>();
                if (alreadySigned != null)
                {
                    signatures.AddRange(alreadySigned);
                }
                var keys =
                    multiSigParams
                    .PubKeys
                    .Select(p => FindKey(ctx, p.ScriptPubKey))
                    .ToArray();

                int sigCount = signatures.Count(s => s != TransactionSignature.Empty && s != null);
                for (int i = 0; i < keys.Length; i++)
                {
                    if (sigCount == multiSigParams.SignatureCount)
                        break;

                    if (i >= signatures.Count)
                    {
                        signatures.Add(null);
                    }
                    if (keys[i] != null)
                    {
                        var sig = txIn.Sign(keys[i], scriptPubKey, ctx.SigHash);
                        signatures[i] = sig;
                        sigCount++;
                    }
                }

                IEnumerable<TransactionSignature> sigs = signatures;
                if (sigCount == multiSigParams.SignatureCount)
                {
                    sigs = sigs.Where(s => s != TransactionSignature.Empty && s != null);
                }

                return PayToMultiSigTemplate.Instance.GenerateScriptSig(sigs);
            }

            var pubKeyParams = PayToPubkeyTemplate.Instance.ExtractScriptPubKeyParameters(scriptPubKey);
            if (pubKeyParams != null)
            {
                var key = FindKey(ctx, scriptPubKey);
                if (key == null)
                    return originalScriptSig;
                var sig = txIn.Sign(key, scriptPubKey, ctx.SigHash);
                return PayToPubkeyTemplate.Instance.GenerateScriptSig(sig);
            }

            throw new NotSupportedException("Unsupported scriptPubKey");
        }


        private Key FindKey(TransactionSigningContext ctx, Script scriptPubKey)
        {
            var key = _Keys
                .Concat(ctx.AdditionalKeys)
                .FirstOrDefault(k => k.PubKey.ScriptPubKey == scriptPubKey ||  //P2PK
                                    k.PubKey.Hash.ScriptPubKey == scriptPubKey || //P2PKH
                                    k.PubKey.ScriptPubKey.Hash.ScriptPubKey == scriptPubKey || //P2PK P2SH
                                    k.PubKey.Hash.ScriptPubKey.Hash.ScriptPubKey == scriptPubKey); //P2PKH P2SH
            if (key == null && KeyFinder != null)
            {
                key = KeyFinder(scriptPubKey);
            }
            return key;
        }

        public TransactionBuilder Then()
        {
            _CurrentGroup = null;
            return this;
        }

        /// <summary>
        /// Specify the amount of money to cover txouts, if not specified all txout will be covered
        /// </summary>
        /// <param name="amount"></param>
        /// <returns></returns>
        public TransactionBuilder CoverOnly(Money amount)
        {
            CurrentGroup.CoverOnly = amount;
            return this;
        }


        Transaction _CompletedTransaction;

        /// <summary>
        /// Allows to keep building on the top of a partially built transaction
        /// </summary>
        /// <param name="transaction">Transaction to complete</param>
        /// <returns></returns>
        public TransactionBuilder ContinueToBuild(Transaction transaction)
        {
            if (_CompletedTransaction != null)
                throw new InvalidOperationException("Transaction to complete already set");
            _CompletedTransaction = transaction.Clone();
            return this;
        }

        /// <summary>
        /// Will cover the remaining amount of TxOut of a partially built transaction (to call after ContinueToBuild)
        /// </summary>
        /// <returns></returns>
        public TransactionBuilder CoverTheRest()
        {
            if (_CompletedTransaction == null)
                throw new InvalidOperationException("A partially built transaction should be specified by calling ContinueToBuild");

            var spent = _CompletedTransaction.Inputs.AsIndexedInputs().Select(txin =>
            {
                var c = FindCoin(txin.PrevOut);
                if (c == null)
                    throw CoinNotFound(txin);
                if (!(c is Coin))
                    return null;
                return (Coin)c;
            })
                    .Where(c => c != null)
                    .Select(c => c.Amount)
                    .Sum();

            var toComplete = _CompletedTransaction.TotalOut - spent;
            CurrentGroup.Builders.Add(ctx =>
            {
                if (toComplete < Money.Zero)
                    return Money.Zero;
                return toComplete;
            });
            return this;
        }

        public TransactionBuilder AddCoins(Transaction transaction)
        {
            var txId = transaction.GetHash();
            AddCoins(transaction.Outputs.Select((o, i) => new Coin(txId, (uint)i, o.Value, o.ScriptPubKey)).ToArray());
            return this;
        }

        Dictionary<ScriptId, Script> _ScriptIdToRedeem = new Dictionary<ScriptId, Script>();
        public TransactionBuilder AddKnownRedeems(params Script[] knownRedeems)
        {
            foreach (var redeem in knownRedeems)
            {
                _ScriptIdToRedeem.AddOrReplace(redeem.Hash, redeem);
            }
            return this;
        }

        public Transaction CombineSignatures(params Transaction[] transactions)
        {
            if (transactions.Length == 1)
                return transactions[0];
            if (transactions.Length == 0)
                return null;

            Transaction tx = transactions[0].Clone();
            for (int i = 1; i < transactions.Length; i++)
            {
                var signed = transactions[i];
                tx = CombineSignaturesCore(tx, signed);
            }
            return tx;
        }

        private Transaction CombineSignaturesCore(Transaction signed1, Transaction signed2)
        {
            if (signed1 == null)
                return signed2;
            if (signed2 == null)
                return signed1;
            var tx = signed1.Clone();
            for (int i = 0; i < tx.Inputs.Count; i++)
            {
                if (i >= signed2.Inputs.Count)
                    break;

                var txIn = tx.Inputs[i];

                var coin = FindCoin(txIn.PrevOut);
                var scriptPubKey = coin == null
                    ? (DeduceScriptPubKey(txIn.ScriptSig) ?? DeduceScriptPubKey(signed2.Inputs[i].ScriptSig))
                    : coin.TxOut.ScriptPubKey;
                tx.Inputs[i].ScriptSig = Script.CombineSignatures(
                                        scriptPubKey,
                                        tx,
                                         i,
                                         signed1.Inputs[i].ScriptSig,
                                         signed2.Inputs[i].ScriptSig);
            }
            return tx;
        }

        private Script DeduceScriptPubKey(Script scriptSig)
        {
            var p2pkh = PayToPubkeyHashTemplate.Instance.ExtractScriptSigParameters(scriptSig);
            if (p2pkh != null && p2pkh.PublicKey != null)
            {
                return p2pkh.PublicKey.Hash.ScriptPubKey;
            }
            var p2sh = PayToScriptHashTemplate.Instance.ExtractScriptSigParameters(scriptSig);
            if (p2sh != null && p2sh.RedeemScript != null)
            {
                return p2sh.RedeemScript.Hash.ScriptPubKey;
            }
            return null;
        }
    }

    public class CoinNotFoundException : KeyNotFoundException
    {
        public CoinNotFoundException(IndexedTxIn txIn)
            : base("No coin matching " + txIn.PrevOut + " was found")
        {
            _OutPoint = txIn.PrevOut;
            _InputIndex = txIn.Index;
        }

        private readonly OutPoint _OutPoint;
        public OutPoint OutPoint
        {
            get
            {
                return _OutPoint;
            }
        }

        private readonly uint _InputIndex;
        public uint InputIndex
        {
            get
            {
                return _InputIndex;
            }
        }
    }
}
