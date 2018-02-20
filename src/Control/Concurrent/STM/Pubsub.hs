module Control.Concurrent.STM.Pubsub
  ( TPub
  , newTPub
  , newTPubIO
  , writeTPub
  , sizeTPub
  , TSub
  , newTSub
  , newTSubIO
  , readTSub
  , sizeTSub
  , isSlowestTSub
  ) where

import Control.Monad.STM (STM, atomically)
import Control.Concurrent.STM.TVar
  (TVar, modifyTVar, newTVar, readTVar, writeTVar)
import Control.Concurrent.STM.TMVar
  (TMVar, mkWeakTMVar, newEmptyTMVar, putTMVar, readTMVar)
import Control.Monad (guard)
import Data.Functor (void)
import GHC.Conc (unsafeIOToSTM)

-- | A 'TPub' is like a write-only broadcast 'Control.Concurrent.STM.TChan',
-- bounded in size by the slowest subscribed 'TSub'.
data TPub a = TPub
  -- A TVar containing the end of the "chain" of TMVars. Invariant: this TVar
  -- always contains an empty TMVar.
  {-# UNPACK #-} !(TVar (Chain a))
  -- The height of the lowest cell that has not yet been garbage collected
  -- (i.e. the slowest subscriber's next cell).
  {-# UNPACK #-} !(TVar Integer)
  -- The maximum length this channel can grow to before sending on it
  -- (publishing) blocks.
  !Integer

data Chain a = Chain
  -- The height of this chain. Invariant: the next cell's tail's height is this
  -- height plus one.
  !Integer
  -- The (possibly empty) cell (head element and tail chain) contained in this
  -- chain.
  {-# UNPACK #-} !(TMVar (Cell a))

data Cell a
  = Cell a !(Chain a)

-- | Create a new 'TPub' with a maximum capacity.
newTPub :: Int -> STM (TPub a)
newTPub size =
  TPub
    <$> (newEmptyTMVar >>= newTVar . Chain 0)
    <*> newTVar 0
    <*> pure (fromIntegral size)

-- | Create a new 'TPub' with a maximum capacity.
newTPubIO :: Int -> IO (TPub a)
newTPubIO size =
  atomically (newTPub size)

-- | Write an element to a 'TPub', which is readable by all subscribed 'TSub's.
-- Retries if the slowest subscribed 'TSub' is sufficiently far behind, per the
-- maximum capacity.
--
-- Because of how deadlock detection interacts with finalizers in GHC as of
-- version @7.8.1@, if this call retries due to a slow subscriber, it will be
-- thrown a 'Control.Exception.BlockedIndefinitelyOnSTM' exception.
--
-- To prevent this, the calling thread must bypass GHC's deadlock detection by
-- creating a 'Foreign.StablePtr.StablePtr' to its own
-- 'Control.Concurrent.ThreadId'.
--
-- Unfortunately, this is a blunt workaround, as GHC will not consider such a
-- thread deadlocked for any reason. This program, for example, will hang
-- forever:
--
-- @
-- 'Control.Concurrent.myThreadId' >>= 'Foreign.StablePtr.newStablePtr'
-- 'Control.Concurrent.newEmptyMVar' >>= 'Control.Concurrent.takeMVar'
-- @
--
-- This should be a perfectly agreeable drawback, however, because deadlock
-- detection should not be relied upon for program correctness, as expounded on
-- in the "Control.Concurrent" documentation.
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

-- | Return the number of elements that have been written to a 'TPub', but not
-- yet read by its slowest 'TSub'.
--
-- If there are no 'TSub's, this number will hover around @0@, as every element
-- written can immediately be garbage collected.
sizeTPub :: TPub a -> STM Integer
sizeTPub (TPub endVar lowestVar _) = do
  Chain height _ <- readTVar endVar
  lowest <- readTVar lowestVar
  pure (height - lowest)

-- Get the height of a 'TPub' (internal).
heightTPub :: TPub a -> STM Integer
heightTPub (TPub endVar _ _) = do
  Chain height _ <- readTVar endVar
  pure height

-- | A 'TSub' is like a read-only 'Control.Concurrent.STM.TChan' created with
-- 'Control.Concurrent.STM.dupTChan'; it begins empty, but can read every
-- element written by the associated 'TPub'.
--
-- An alive 'TSub' will cause writes to the associated 'TPub' to block if the
-- 'TSub' is sufficiently far behind, per the `TPub`'s maximum capacity.
--
-- A 'TSub' "unsubscribes" automatically when it is garbage collected.
data TSub a = TSub
  -- The chain of elements to read.
  {-# UNPACK #-} !(TVar (Chain a))
  -- A reference to the publisher itself.
  !(TPub a)

-- | Subscribe to a 'TPub'.
newTSub :: TPub a -> STM (TSub a)
newTSub pub@(TPub endVar _ _) = do
  hole <- readTVar endVar
  headVar <- newTVar hole
  pure (TSub headVar pub)

-- | Subscribe to a 'TPub'.
newTSubIO :: TPub a -> IO (TSub a)
newTSubIO pub =
  atomically (newTSub pub)

-- | Read the next value written by a `TSub`'s associated 'TPub'.
readTSub :: TSub a -> STM a
readTSub (TSub chainVar _) = do
  Chain _ cellVar <- readTVar chainVar
  Cell val newChain <- readTMVar cellVar
  writeTVar chainVar newChain
  pure val

-- | Return the number of unread elements written by a `TSub`'s associated
-- 'TPub'.
sizeTSub :: TSub a -> STM Integer
sizeTSub (TSub chainVar pub) = do
  Chain height _ <- readTVar chainVar
  highest <- heightTPub pub
  pure (highest - height)

-- | Is a 'TSub' among the slowest subscribers?
isSlowestTSub :: TSub a -> STM Bool
isSlowestTSub (TSub endVar pub) = do
  Chain height _ <- readTVar endVar
  highest <- heightTPub pub
  size <- sizeTPub pub
  pure (height == highest - size)
