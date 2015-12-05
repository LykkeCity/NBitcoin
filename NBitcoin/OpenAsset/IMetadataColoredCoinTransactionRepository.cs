using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.OpenAsset
{
    /// <summary>
    /// The interface used store the actual colored coin metadata to the repository
    /// </summary>
    public interface IMetadataColoredCoinTransactionRepository
    {
        void AddMetadataToRepository(string metadata);
    }
}
