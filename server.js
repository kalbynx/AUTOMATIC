const express = require('express');
const cron = require('node-cron');
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Supabase configuration
const supabaseUrl = "https://evberyanshxxalxtwnnc.supabase.co";
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV2YmVyeWFuc2h4eGFseHR3bm5jIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQwODMwOTcsImV4cCI6MjA1OTY1OTA5N30.pEoPiIi78Tvl5URw0Xy_vAxsd-3XqRlC8FTnX9HpgMw";
const supabase = createClient(supabaseUrl, supabaseKey);

// Payment verification API configuration
const PAYMENT_API_BASE_URL = 'https://verifyapi.leulzenebe.pro';
const API_KEY = 'Y21icm5reDVuMDAxbG5xMGs3b3RueWI2dC0xNzQ5NjI4NDI1MDI1LWFlMG9zcDZzcTNy';

// Expected payment recipient details
const EXPECTED_RECIPIENTS = {
    telebirr: {
        name: "Kal Eab Beh",
        accountPattern: "2519****1822"
    },
    cbe: {
        name: "Kaleabe Beharneh Mekasha",
        accountPattern: "1****3678"
    }
};

function validatePaymentRecipient(paymentData, method) {
    if (method === 'telebirr') {
        const creditedName = paymentData.creditedPartyName;
        const creditedAccount = paymentData.creditedPartyAccountNo;
        return (
            creditedName === EXPECTED_RECIPIENTS.telebirr.name &&
            creditedAccount === EXPECTED_RECIPIENTS.telebirr.accountPattern
        );
    } else if (method === 'cbe') {
        const receiver = paymentData.receiver;
        const receiverAccount = paymentData.receiverAccount;
        return (
            receiver === EXPECTED_RECIPIENTS.cbe.name &&
            receiverAccount === EXPECTED_RECIPIENTS.cbe.accountPattern
        );
    }
    return false;
}

// Retry configuration
const RETRY_CONFIG = {
    maxRetries: 5,
    retryDelay: 2000, // 2 seconds
    backoffMultiplier: 1.5
};

app.use(express.json());

// Logging function
function log(message, type = 'INFO') {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] [${type}] ${message}`);
}

// Sleep function for delays
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Extract transaction ID from description
function extractTransactionId(description) {
    const match = description.match(/TxID:\s*([A-Za-z0-9]+)/);
    return match ? match[1] : null;
}

// Determine payment method from description
function getPaymentMethod(description) {
    if (description.toLowerCase().includes('telebirr')) {
        return 'telebirr';
    } else if (description.toLowerCase().includes('cbe')) {
        return 'cbe';
    }
    return 'unknown';
}

// Verify Telebirr payment with retry logic
async function verifyTelebirrPayment(reference, retryCount = 0) {
    try {
        const response = await axios.post(`${PAYMENT_API_BASE_URL}/verify-telebirr`, {
            reference: reference
        }, {
            headers: {
                'x-api-key': API_KEY,
                'Content-Type': 'application/json'
            },
            timeout: 30000
        });

        return response.data;
    } catch (error) {
        log(`Telebirr verification error for ${reference} (attempt ${retryCount + 1}): ${error.message}`, 'ERROR');
        
        if (retryCount < RETRY_CONFIG.maxRetries) {
            const delay = RETRY_CONFIG.retryDelay * Math.pow(RETRY_CONFIG.backoffMultiplier, retryCount);
            log(`Retrying Telebirr verification in ${delay}ms...`, 'INFO');
            await sleep(delay);
            return verifyTelebirrPayment(reference, retryCount + 1);
        }
        
        throw error;
    }
}

// Verify CBE payment with retry logic
async function verifyCBEPayment(reference, accountSuffix = "85963678", retryCount = 0) {
    try {
        const response = await axios.post(`${PAYMENT_API_BASE_URL}/verify-cbe`, {
            reference: reference,
            accountSuffix: accountSuffix
        }, {
            headers: {
                'x-api-key': API_KEY,
                'Content-Type': 'application/json'
            },
            timeout: 30000
        });

        return response.data;
    } catch (error) {
        log(`CBE verification error for ${reference} (attempt ${retryCount + 1}): ${error.message}`, 'ERROR');
        
        if (retryCount < RETRY_CONFIG.maxRetries) {
            const delay = RETRY_CONFIG.retryDelay * Math.pow(RETRY_CONFIG.backoffMultiplier, retryCount);
            log(`Retrying CBE verification in ${delay}ms...`, 'INFO');
            await sleep(delay);
            return verifyCBEPayment(reference, accountSuffix, retryCount + 1);
        }
        
        throw error;
    }
}


// Check if transaction ID has already been used
async function isTransactionIdUsed(transactionId) {
    try {
        const { data, error } = await supabase
            .from('used_transaction_ids')
            .select('id')
            .eq('transaction_id', transactionId)
            .single();

        if (error && error.code !== 'PGRST116') { // Ignore "not found" error
            log(`Error checking transaction ID ${transactionId}: ${error.message}`, 'ERROR');
            return false; // Assume not used if error occurs
        }

        return !!data; // Returns true if found, false if not
    } catch (error) {
        log(`Error in isTransactionIdUsed: ${error.message}`, 'ERROR');
        return false; // Assume not used if error occurs
    }
}

// Mark a transaction ID as used
async function markTransactionIdAsUsed(transactionId, depositId, amount) {
    try {
        const { error } = await supabase
            .from('used_transaction_ids')
            .insert({
                transaction_id: transactionId,
                deposit_id: depositId,
                amount: amount,
                used_at: new Date().toISOString()
            });

        if (error) {
            log(`Error marking transaction ID ${transactionId} as used: ${error.message}`, 'ERROR');
            return false;
        }

        log(`Transaction ID ${transactionId} marked as used`, 'INFO');
        return true;
    } catch (error) {
        log(`Error in markTransactionIdAsUsed: ${error.message}`, 'ERROR');
        return false;
    }
}

// Reject a deposit with reason
async function rejectDeposit(deposit, reason) {
    try {
        const { error } = await supabase
            .from('player_transactions')
            .update({
                status: 'rejected',
                description: `Rejected: ${reason}`
            })
            .eq('id', deposit.id);

        if (error) {
            log(`Error rejecting deposit ${deposit.id}: ${error.message}`, 'ERROR');
            return;
        }

        log(`Deposit ${deposit.id} rejected: ${reason}`, 'INFO');
    } catch (error) {
        log(`Error in rejectDeposit: ${error.message}`, 'ERROR');
    }
}

// Process pending deposits
async function processPendingDeposits() {
    try {
        log('Starting to process pending deposits...');
        
        // Get all pending deposit transactions
        const { data: pendingDeposits, error } = await supabase
            .from('player_transactions')
            .select('*')
            .eq('transaction_type', 'deposit')
            .eq('status', 'pending')
            .order('created_at', { ascending: true });

        if (error) {
            log(`Error fetching pending deposits: ${error.message}`, 'ERROR');
            return;
        }

        if (!pendingDeposits || pendingDeposits.length === 0) {
            log('No pending deposits found');
            return;
        }

        log(`Found ${pendingDeposits.length} pending deposits to process`);

        for (const deposit of pendingDeposits) {
            await processIndividualDeposit(deposit);
            // Add delay between processing to avoid rate limiting
            await sleep(2000);
        }

    } catch (error) {
        log(`Error in processPendingDeposits: ${error.message}`, 'ERROR');
    }
}

// Process individual deposit
async function processIndividualDeposit(deposit) {
    try {
        log(`Processing deposit ID: ${deposit.id} for ${deposit.player_phone}`);
        
        const transactionId = extractTransactionId(deposit.description);
        if (!transactionId) {
            log(`Invalid transaction ID format for deposit ${deposit.id}`, 'ERROR');
            await rejectDeposit(deposit, 'Invalid transaction ID format');
            return;
        }

        // Check if transaction ID has already been used
        const isUsed = await isTransactionIdUsed(transactionId);
        if (isUsed) {
            log(`Transaction ID ${transactionId} has already been used - rejecting deposit ${deposit.id}`, 'WARN');
            await rejectDeposit(deposit, 'This transaction ID has already been used');
            return;
        }

        const paymentMethod = getPaymentMethod(deposit.description);
        if (paymentMethod === 'unknown') {
            log(`Unknown payment method for deposit ${deposit.id}`, 'ERROR');
            await rejectDeposit(deposit, 'Unknown payment method');
            return;
        }

        let paymentData;
        
        try {
            // Verify payment based on method with retry logic
            if (paymentMethod === 'telebirr') {
                paymentData = await verifyTelebirrPayment(transactionId);
            } else if (paymentMethod === 'cbe') {
                paymentData = await verifyCBEPayment(transactionId);
            }
        } catch (error) {
            log(`Failed to verify payment after ${RETRY_CONFIG.maxRetries} retries for deposit ${deposit.id}. Leaving as pending.`, 'ERROR');
            return; // Don't reject, leave as pending
        }

        if (!paymentData || !paymentData.success) {
            log(`Payment verification failed for deposit ${deposit.id} - transaction not found. Leaving as pending.`, 'WARN');
            return; // Don't reject, leave as pending
        }

        // Get payment amount based on method
       if (!paymentData || !paymentData.data) {
    log(`paymentData or paymentData.data is undefined for deposit ${deposit.id}`, 'ERROR');
    await rejectDeposit(deposit, 'Invalid payment data received from verification API');
    return;
}

let paidAmount;
if (paymentMethod === 'telebirr') {
    paidAmount = parseFloat(paymentData.data.settledAmount || paymentData.data.amount || 0);
} else if (paymentMethod === 'cbe') {
    paidAmount = parseFloat(paymentData.data.amount || 0);
}
        // Check if recipient matches (but don't reject if it doesn't)
        const recipientMatches = validatePaymentRecipient(paymentData.data, paymentMethod);
        
        if (!recipientMatches) {
            // If recipient doesn't match, approve the confirmed amount with special description
            await approveDepositWithConfirmedAmount(deposit, paymentData.data, paidAmount, transactionId);
            return;
        }

        // Check if amounts match
        if (Math.abs(paidAmount - deposit.amount) > 0.01) {
            // If amounts don't match, approve the confirmed amount
            await approveDepositWithConfirmedAmount(deposit, paymentData.data, paidAmount, transactionId);
            return;
        }

        // All checks passed - approve the deposit normally
        await approveDeposit(deposit, paymentData.data, paidAmount, transactionId);

    } catch (error) {
        log(`Error processing deposit ${deposit.id}: ${error.message}`, 'ERROR');
        // Don't reject on system errors, just log and continue
    }
}

// Approve deposit with confirmed amount (for mismatches)
async function approveDepositWithConfirmedAmount(deposit, paymentData, confirmedAmount, transactionId) {
    try {
        log(`Approving deposit ${deposit.id} with confirmed amount ${confirmedAmount} ETB`);

        // Get current user balance
        const { data: userData, error: userError } = await supabase
            .from('users')
            .select('balance')
            .eq('phone', deposit.player_phone)
            .single();

        if (userError) {
            log(`Error fetching user data: ${userError.message}`, 'ERROR');
            return;
        }

        const newBalance = userData.balance + confirmedAmount;

        // Update user balance
        const { error: balanceError } = await supabase
            .from('users')
            .update({ balance: newBalance })
            .eq('phone', deposit.player_phone);

        if (balanceError) {
            log(`Error updating balance: ${balanceError.message}`, 'ERROR');
            return;
        }

        // Update transaction with confirmed amount and special description
        const { error: transactionError } = await supabase
            .from('player_transactions')
            .update({
                status: 'approved',
                amount: confirmedAmount, // Update to confirmed amount
                balance_after: newBalance,
                description: `Only ${confirmedAmount} ETB was sent`
            })
            .eq('id', deposit.id);

        if (transactionError) {
            log(`Error updating transaction: ${transactionError.message}`, 'ERROR');
            return;
        }

        // Mark transaction ID as used
        await markTransactionIdAsUsed(transactionId, deposit.id, confirmedAmount);

        log(`Successfully approved deposit ${deposit.id} with confirmed amount ${confirmedAmount} ETB - Balance updated to ${newBalance} ETB`);

    } catch (error) {
        log(`Error approving deposit with confirmed amount: ${error.message}`, 'ERROR');
    }
}

// Approve deposit (normal approval)
// Updated approveDeposit function with transaction locking
async function approveDeposit(deposit) {
  const transactionId = deposit.id;
  console.log(`Starting approval for deposit ${transactionId}`);

  try {
    // Begin a database transaction
    const { data: transactionData, error: transactionError } = await supabase.rpc('begin_transaction');

    if (transactionError) throw transactionError;

    // 1. Get current balance WITH LOCK
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('balance')
      .eq('phone', deposit.player_phone)
      .single()
      .forUpdate(); // This locks the row

    if (userError) throw userError;

    // 2. Calculate new balance
    const currentBalance = userData.balance || 0;
    const newBalance = currentBalance + deposit.amount;
    
    console.log(`Updating balance for ${deposit.player_phone} from ${currentBalance} to ${newBalance}`);

    // 3. Update user balance
    const { error: updateError } = await supabase
      .from('users')
      .update({ balance: newBalance })
      .eq('phone', deposit.player_phone);

    if (updateError) throw updateError;

    // 4. Update transaction status
    const { error: transactionUpdateError } = await supabase
      .from('player_transactions')
      .update({
        status: 'approved',
        balance_after: newBalance,
        description: `Approved - Added ${deposit.amount} ETB`
      })
      .eq('id', transactionId);

    if (transactionUpdateError) throw transactionUpdateError;

    // Commit the transaction
    const { error: commitError } = await supabase.rpc('commit_transaction');
    if (commitError) throw commitError;

    console.log(`✅ Deposit ${transactionId} approved successfully`);
    return true;
  } catch (error) {
    // Rollback on error
    await supabase.rpc('rollback_transaction').catch(rollbackError => {
      console.error('Rollback failed:', rollbackError);
    });

    console.error(`❌ Failed to approve deposit ${transactionId}:`, error.message);
    
    // Mark as failed
    await supabase
      .from('player_transactions')
      .update({
        status: 'failed',
        description: `Approval failed: ${error.message}`
      })
      .eq('id', transactionId);

    return false;
  }
}

// Set up real-time Supabase listener for pending transactions
function setupRealtimeListener() {
    log('Setting up real-time listener for pending transactions...');
    
    const channel = supabase
        .channel('pending_deposits')
        .on(
            'postgres_changes',
            {
                event: 'INSERT',
                schema: 'public',
                table: 'player_transactions',
                filter: 'transaction_type=eq.deposit'
            },
            (payload) => {
                log(`New deposit transaction detected: ${payload.new.id}`);
                if (payload.new.status === 'pending') {
                    log(`Processing new pending deposit: ${payload.new.id}`);
                    // Process the new deposit immediately
                    setTimeout(() => {
                        processIndividualDeposit(payload.new);
                    }, 1000); // Small delay to ensure transaction is fully committed
                }
            }
        )
        .on(
            'postgres_changes',
            {
                event: 'UPDATE',
                schema: 'public',
                table: 'player_transactions',
                filter: 'transaction_type=eq.deposit'
            },
            (payload) => {
                if (payload.new.status === 'pending' && payload.old.status !== 'pending') {
                    log(`Transaction ${payload.new.id} changed to pending status`);
                    setTimeout(() => {
                        processIndividualDeposit(payload.new);
                    }, 1000);
                }
            }
        )
        .subscribe((status) => {
            if (status === 'SUBSCRIBED') {
                log('Successfully subscribed to real-time updates');
            } else if (status === 'CHANNEL_ERROR') {
                log('Error subscribing to real-time updates', 'ERROR');
            }
        });

    return channel;
}

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        service: 'Payment Verification Server',
        retryConfig: RETRY_CONFIG
    });
});

// Manual trigger endpoint for testing
app.post('/process-deposits', async (req, res) => {
    try {
        await processPendingDeposits();
        res.json({ success: true, message: 'Deposit processing triggered' });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Status endpoint
app.get('/status', async (req, res) => {
    try {
        const { data: pendingCount } = await supabase
            .from('player_transactions')
            .select('id', { count: 'exact' })
            .eq('transaction_type', 'deposit')
            .eq('status', 'pending');

        res.json({
            status: 'running',
            pendingDeposits: pendingCount?.length || 0,
            lastCheck: new Date().toISOString(),
            realtimeEnabled: true,
            retryConfig: RETRY_CONFIG
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Schedule automatic processing every 5 minutes (as backup to real-time)
cron.schedule('*/5 * * * *', () => {
    log('Running scheduled deposit processing (backup check)...');
    processPendingDeposits();
});

// Start server
app.listen(PORT, () => {
    log(`Payment verification server running on port ${PORT}`);
    log('Real-time processing: Enabled');
    log('Scheduled processing: Every 5 minutes (backup)');
    log('Manual trigger: POST /process-deposits');
    log('Status check: GET /status');
    log(`Retry configuration: Max ${RETRY_CONFIG.maxRetries} retries with ${RETRY_CONFIG.retryDelay}ms base delay`);
    
    // Set up real-time listener
    const realtimeChannel = setupRealtimeListener();
    
    // Store channel reference for cleanup
    global.realtimeChannel = realtimeChannel;
    
    // Run initial processing
    setTimeout(() => {
        log('Running initial deposit processing...');
        processPendingDeposits();
    }, 5000);
});

// Graceful shutdown
process.on('SIGINT', () => {
    log('Shutting down payment verification server...');
    if (global.realtimeChannel) {
        supabase.removeChannel(global.realtimeChannel);
        log('Real-time channel closed');
    }
    process.exit(0);
});

process.on('SIGTERM', () => {
    log('Shutting down payment verification server...');
    if (global.realtimeChannel) {
        supabase.removeChannel(global.realtimeChannel);
        log('Real-time channel closed');
    }
    process.exit(0);
});