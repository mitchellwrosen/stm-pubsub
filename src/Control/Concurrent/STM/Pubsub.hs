module Control.Concurrent.STM.Pubsub
  ( TPub
  , newTPub
  , writeTPub
  , sizeTPub
  , TSub
  , newTSub
  , readTSub
  ) where

import Control.Concurrent.STM
import GHC.Conc (unsafeIOToSTM)
import Data.Functor
import Control.Monad

-- A 'TPub' is like a write-only 'TBQueue', bounded in size by the slowest
-- 'TSub' subscriber.
data TPub a = TPub
  -- A TVar containing the end of the "chain" of TMVars. Invariant: this TVar
  -- always contains an empty TMVar.
  {-# UNPACK #-} !(TVar (Chain a))
  -- The height of the lowest cell that has not yet been garbage collected
  -- (i.e. the slowest subscriber's next cell).
  {-# UNPACK #-} !(TVar Integer)
  -- The maximum length this channel can grow to before sending on it
  -- (publishing) blocks.
  {-# UNPACK #-} !Integer

data Chain a = Chain
  -- The height of this chain. Invariant: the next cell's tail's height is this
  -- height plus one.
  {-# UNPACK #-} !Integer
  -- The (possibly empty) cell (head element and tail chain) contained in this
  -- chain.
  {-# UNPACK #-} !(TMVar (Cell a))

data Cell a
  = Cell a !(Chain a)

-- | Create a new 'TPub' of the given maximum @size@.
newTPub :: Int -> STM (TPub a)
newTPub size =
  TPub
    <$> (newEmptyTMVar >>= newTVar . Chain 0)
    <*> newTVar 0
    <*> pure (fromIntegral size)

-- | Write an element to a 'TPub', which is readable by all connected 'TSub's.
-- Retries if the slowest subscriber is @size@ elements behind.
writeTPub :: TPub a -> a -> STM ()
writeTPub (TPub endVar lowestVar size) x = do
  -- Retry if we've reached the maximum size.
  Chain height cellVar <- readTVar endVar
  lowest <- readTVar lowestVar
  guard (height - lowest < size)

  -- Replace the old chain's empty cell with a one-elem chain.
  hole <- newEmptyTMVar
  let newEnd = Chain (height+1) hole
  putTMVar cellVar (Cell x newEnd)

  writeTVar endVar newEnd

  -- When the old cell is garbage collected, bump the TPub's lowest height. This
  -- finalizer is idempotent, thus safe to run multiple times if this
  -- transaction retries.
  --
  -- Annoyingly, if (say) 100 cells are garbage collected, they are always
  -- garbage collected in reverse order. So, only the *first* finalizer that
  -- runs is relevant, and the rest are no-ops, because e.g. 'max 99 100 = 100',
  -- 'max 98 100 = 100', etc.
  --
  -- Would be nice to be able to cancel a finalizer, somehow.
  unsafeIOToSTM
    (void
      (mkWeakTMVar cellVar
        (atomically (modifyTVar lowestVar (max (height+1))))))

-- | Return the number of elements that have been written but not yet read by
-- the slowest subscriber.
--
-- If there are no subscribers, this number will hover around @0@, per how often
-- garbage collections are run.
sizeTPub :: TPub a -> STM Integer
sizeTPub (TPub endVar lowestVar _) = do
  Chain height _ <- readTVar endVar
  lowest <- readTVar lowestVar
  pure (height - lowest)

newtype TSub a
  = TSub (TVar (Chain a))

-- | Subscribe to a 'TPub'.
newTSub :: TPub a -> STM (TSub a)
newTSub (TPub endVar _ _) = do
  hole <- readTVar endVar
  headVar <- newTVar hole
  pure (TSub headVar)

-- | Read a value from a 'TSub'.
readTSub :: TSub a -> STM a
readTSub (TSub chainVar) = do
  Chain _ cellVar <- readTVar chainVar
  Cell val newChain <- readTMVar cellVar
  writeTVar chainVar newChain
  pure val
