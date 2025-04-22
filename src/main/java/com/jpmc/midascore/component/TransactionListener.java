package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class TransactionListener {
    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    @KafkaListener(topics = "${general.kafka-topic}",groupId = "midas-group")
    public void listen(Transaction transaction) {
        System.out.println("Received transaction: " + transaction);
        Optional<UserRecord> senderOpt = Optional.ofNullable(userRepository.findById(transaction.getSenderId()));
        Optional<UserRecord> recipientOpt = Optional.ofNullable(userRepository.findById(transaction.getRecipientId()));

        if (senderOpt.isPresent() && recipientOpt.isPresent()) {
            UserRecord sender = senderOpt.get();
            UserRecord recipient = recipientOpt.get();

            if (sender.getBalance() >= transaction.getAmount()) {
                sender.setBalance(sender.getBalance() - transaction.getAmount());
                recipient.setBalance(recipient.getBalance() + transaction.getAmount());

                userRepository.save(sender);
                userRepository.save(recipient);

                TransactionRecord record = new TransactionRecord();
                record.setSender(sender);
                record.setRecipient(recipient);
                record.setAmount(transaction.getAmount());

                transactionRecordRepository.save(record);

                if(sender.getName().equals("waldorf")){
                    System.out.println("**** send waldorf bal:"+ sender.getBalance());
                }
                if(recipient.getName().equals("waldorf")){
                    System.out.println("**** reci waldorf bal:"+ recipient.getBalance());
                }

                System.out.println("Transaction successful");
            } else {
                System.out.println("Sender has insufficient balance.");
            }
        } else {
            System.out.println("Invalid sender or recipient.");
        }
    }
}
