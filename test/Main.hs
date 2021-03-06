import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.Pubsub
import Control.Exception
import Control.Monad
import Foreign.StablePtr
import System.Mem (performGC)
import Test.Hspec

main :: IO ()
main = do
  -- Keep main thread alive through deadlocks, see note at the bottom of
  -- Control.Concurrent
  void (myThreadId >>= newStablePtr)
  hspec spec

spec :: Spec
spec = do
  describe "writeTPub" $ do
    it "respects max size" $ do
      pub <- newTPubIO 1 :: IO (TPub Int)
      atomically (writeTPub pub 1)
      atomically ((writeTPub pub 2 >> pure True) <|> pure False)
        `shouldReturn` False

    it "unblocks as subscribers catch up" $ do
      pub <- newTPubIO 1 :: IO (TPub Int)
      sub <- atomically (newTSub pub)

      -- Subscriber is slow, so we know publisher is actually retrying writes
      _ <-
        forkIO
          (replicateM_ 100 $ do
            threadDelay 100
            atomically (readTSub sub))

      replicateM_ 100 (atomically (writeTPub pub 1))

  describe "sizeTPub" $ do
    it "evaluates size using the slowest subscriber" $ do
      pub <- newTPubIO 200 :: IO (TPub Int)
      sub1 <- atomically (newTSub pub)
      sub2 <- atomically (newTSub pub)

      atomically (replicateM_ 200 (writeTPub pub 1))

      atomically (sizeTPub pub)
        `shouldReturn` 200

      atomically (replicateM_ 100 (readTSub sub1))

      sync

      atomically (sizeTPub pub)
        `shouldReturn` 200

      atomically (replicateM_ 150 (readTSub sub2))

      sync

      atomically (sizeTPub pub)
        `shouldReturn` 100

      -- Keep unread elements alive
      _ <- atomically (readTSub sub1)
      _ <- atomically (readTSub sub2)
      pure ()

  describe "sizeTSub" $ do
    it "reports size accurately" $ do
      pub <- newTPubIO 200 :: IO (TPub Int)
      sub <- atomically (newTSub pub)

      atomically (replicateM_ 200 (writeTPub pub 1))

      atomically (sizeTSub sub)
        `shouldReturn` 200

      atomically (replicateM_ 100 (readTSub sub))

      sync

      atomically (sizeTSub sub)
        `shouldReturn` 100

      -- Keep unread elements alive
      _ <- atomically (readTSub sub)
      pure ()

  describe "isSlowestTSub" $ do
    it "determines if a subscriber is among the slowest" $ do
      pub <- newTPubIO 200 :: IO (TPub Int)
      sub1 <- atomically (newTSub pub)
      sub2 <- atomically (newTSub pub)

      atomically (isSlowestTSub sub1)
        `shouldReturn` True
      atomically (isSlowestTSub sub2)
        `shouldReturn` True

      atomically (replicateM_ 200 (writeTPub pub 1))

      atomically (replicateM_ 100 (readTSub sub1))

      sync

      atomically (isSlowestTSub sub1)
        `shouldReturn` False
      atomically (isSlowestTSub sub2)
        `shouldReturn` True

      atomically (replicateM_ 100 (readTSub sub2))

      sync

      atomically (isSlowestTSub sub1)
        `shouldReturn` True
      atomically (isSlowestTSub sub2)
        `shouldReturn` True

      -- Keep unread elements alive
      _ <- atomically (readTSub sub1)
      _ <- atomically (readTSub sub2)
      pure ()

sync :: IO ()
sync = do
  performGC
  threadDelay (100*1000) -- Give some time for finalizers to be scheduled
