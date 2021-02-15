# Template based on https://www.youtube.com/watch?v=G3pOvrKkFuk

import nlp
import pytorch_lightning as pl
#import sh
import torch
import transformers

#sh.rm("-r", "-f", "logs")
#sh.mkdir("logs")

EPOCHS = 10
LR = 1e-2
MOMENTUM = 0.9
MODEL = "bert-base-uncased"
SEQ_LENGTH = 32
BATCH_SIZE = 8

PERCENT_DATA = 100


class HateSpeechClassifier(pl.LightningModule):

    def __init__(self):
        super().__init__()
        self.model = transformers.BertForSequenceClassification.from_pretrained(MODEL)
        self.loss = torch.nn.CrossEntropyLoss(reduction="none")

    def prepare_data(self):
        tokenizer = transformers.BertTokenizerFast.from_pretrained(MODEL)

        def _tokenize(x):
            x["input_ids"] = tokenizer.batch_encode_plus(x["text"],
                                                         max_length=SEQ_LENGTH,
                                                         pad_to_max_length=True)["input_ids"]
            return x

        def _prepare_ds(split):
            ds = nlp.load_dataset("imdb", split=f"{split}[:{PERCENT_DATA}%]")
            ds = ds.map(_tokenize, batched=True)
            ds.set_format(type="torch", columns=["input_ids", "label"])
            return ds

        self.train_ds = _prepare_ds("train")
        self.test_ds = _prepare_ds("test")

    def forward(self, input_ids):
        mask = (input_ids != 0).float()
        logits, = self.model(input_ids, mask, return_dict=False)
        return logits

    def training_step(self, batch, batch_idx):
        logits = self.forward(batch["input_ids"])
        loss = self.loss(logits, batch["label"]).mean()
        return {"loss": loss, "log": {"train_loss": loss}}
        pass

    def validation_step(self, batch, batch_idx):
        logits = self.forward(batch["input_ids"])
        loss = self.loss(logits, batch["label"])
        accuracy = (logits.argmax(-1) == batch["label"]).float()
        return {"loss": loss, "accuracy": accuracy}

    def validation_epoch_end(self, outputs):
        loss = torch.cat([o["loss"] for o in outputs], 0).mean()
        accuracy = torch.cat([o["accuracy"] for o in outputs], 0).mean()
        out = {"val_loss": loss, "val_acc": accuracy}
        return {**out, "log": out}

    def train_dataloader(self):
        return torch.utils.data.DataLoader(self.train_ds,
                                           batch_size=BATCH_SIZE,
                                           drop_last=True,
                                           shuffle=True)

    def val_dataloader(self):
        return torch.utils.data.DataLoader(self.test_ds,
                                           batch_size=BATCH_SIZE,
                                           drop_last=False,
                                           shuffle=False)

    def configure_optimizers(self):
        return torch.optim.SGD(
            self.parameters(),
            lr=LR,
            momentum=MOMENTUM
        )


def main():
    model = HateSpeechClassifier()
    trainer = pl.Trainer(
        default_root_dir="logs",
        gpus=(1 if torch.cuda.is_available() else 0),
        max_epochs=EPOCHS,
        logger=pl.loggers.TensorBoardLogger("logs/", name="imdb", version=0)
    )
    trainer.fit(model)


if __name__ == '__main__':
    main()
